/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming.async;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Handles the inbound side of streaming messages and sstable data. From the incoming data, we derserialize the message
 * and potentially reify partitions and rows and write those out to new sstable files. Because deserialization is a blocking affair,
 * we can't block the netty event loop. Thus we have a background thread perform all the blocking deserialization.
 */
public class StreamingInboundHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 15;
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 16;

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    private final StreamSession session;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     * <p>
     * For thread safety, this structure's resources are released on the consuming thread
     * (via {@link RebufferingByteBufDataInputPlus#close()},
     * but the producing side calls {@link RebufferingByteBufDataInputPlus#markClose()} to notify the input that is should close.
     */
    private RebufferingByteBufDataInputPlus buffers;

    private volatile boolean closed;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion, @Nullable StreamSession session)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        this.session = session;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        buffers = new RebufferingByteBufDataInputPlus(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx.channel().config());
        Thread blockingIOThread = new FastThreadLocalThread(new StreamDeserializingTask(session, ctx),
                                                            String.format("Stream-Deserializer-%s-%s", ctx.channel().id(), remoteAddress.toString()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception
    {
        if (!closed && message instanceof ByteBuf)
            buffers.append((ByteBuf) message);
        else
            ctx.fireChannelRead(message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws IOException
    {
        close();
        ctx.fireChannelInactive();
    }

    private void close()
    {
        closed = true;
        buffers.markClose();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing streaming file", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

    /**
     * For testing only!!
     */
    void setPendingBuffers(RebufferingByteBufDataInputPlus bufChannel)
    {
        this.buffers = bufChannel;
    }

    /**
     * The task that performs the actual deserialization.
     */
    private class StreamDeserializingTask implements Runnable
    {
        private final ChannelHandlerContext ctx;

        private StreamSession session;

        StreamDeserializingTask(StreamSession session, ChannelHandlerContext ctx)
        {
            this.session = session;
            this.ctx = ctx;
        }

        @Override
        public void run()
        {
            try
            {
                while (!closed)
                {
                    // do a check of available bytes and possibly sleep some amount of time (then continue).
                    // this way we can break out of run() sanely or we end up blocking indefintely in StreamMessage.deserialize()
                    while (buffers.available() == 0)
                    {
                        Uninterruptibles.sleepUninterruptibly(400, TimeUnit.MILLISECONDS);
                        if (closed || !ctx.channel().isOpen())
                            return;
                    }

                    StreamMessage message = StreamMessage.deserialize(buffers, protocolVersion, null);

                    // StreamInitMessage starts a new channel, and IncomingFileMessage potentially, as well.
                    // IncomingFileMessage needs a session to be established a priori, though
                    if (message instanceof StreamInitMessage)
                    {
                        assert session == null : "initiator of stream session received a StreamInitMessage";
                        StreamInitMessage init = (StreamInitMessage) message;
                        StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.description, init.from, ctx.channel(), init.keepSSTableLevel, init.isIncremental, init.pendingRepair);
                        session = StreamManager.instance.findSession(init.from, init.planId, init.sessionIndex);
                    }
                    else if (message instanceof IncomingFileMessage)
                    {
                        // TODO: it'd be great to check if the session actually exists before slurping in the entire sstable,
                        // but that's a refactoring for another day
                        FileMessageHeader header = ((IncomingFileMessage) message).header;
                        session = StreamManager.instance.findSession(header.sender, header.planId, header.sessionIndex);
                    }

                    if (session == null)
                        throw new IllegalStateException("no session found for message " + message);

                    logger.debug("[Stream #{}] Received {}", session.planId(), message);

                    session.messageReceived(message);
                }
            }
            catch (EOFException eof)
            {
                // ignore
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                if (session != null)
                    session.onError(t);
                else
                    logger.error("failed to deserialize an incoming streaming message", t);
            }
            finally
            {
                ctx.close();
                closed = true;

                if (buffers != null)
                    buffers.close();
            }
        }
    }
}
