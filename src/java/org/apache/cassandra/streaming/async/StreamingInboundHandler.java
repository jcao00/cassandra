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

/**
 * Handles the inbound side of streaming sstable data. From the incoming data, we reify partitions and rows
 * and write those out to new sstable files. Because deserialization is a blocking affair, we can't block
 * the netty event loop. Thus we have a background thread perform all the blocking deserialization.
 *
 *
 *  * // TODO:JEB document
 * - netty low/high water marks & how it relates to our use of auto-read
 * - netty auto-read, and why we do it on the event loop (keeps things simple from the netty dispatch POV)
 */
public class StreamingInboundHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 15;
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 16;

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     */
    private RebufferingByteBufDataInputPlus buffers;

    /**
     * A background thread that performs the deserialization of the sstable data.
     */
    private Thread blockingIOThread;

    private volatile boolean closed;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        buffers = new RebufferingByteBufDataInputPlus(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx.channel().config());
        blockingIOThread = new FastThreadLocalThread(new StreamDeserializingTask(ctx),
                                                     String.format("Stream-Inbound--%s-%s", ctx.channel().id(), remoteAddress.toString()));
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
     */
    private class StreamDeserializingTask implements Runnable
    {
        private final ChannelHandlerContext ctx;

        private StreamSession session;

        StreamDeserializingTask(ChannelHandlerContext ctx)
        {
            this.ctx = ctx;
        }

        @Override
        public void run()
        {
            try
            {
                while (!closed)
                {
                    StreamMessage message = StreamMessage.deserialize(buffers, protocolVersion, null);
                    if (message == null)
                        continue;

                    // StreamInitMessage & IncomingFileMessage each start new channels, and IMF needs a session to be established a priori
                    if (message instanceof StreamInitMessage)
                    {
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

                    // TODO:JEB better error handling
                    if (session == null)
                        throw new IllegalStateException("no session found for message " + message);

                    logger.debug("[Stream #{}] Received {}", session.planId(), message);

                    session.messageReceived(message);
                }
            }
            catch (EOFException e)
            {
                logger.debug("eof reading from socket; closing", e);
            }
            catch (Throwable t)
            {
                // TODO:JEB do we close the session or send complete somewheres on fail?

                // Throwable can be Runtime error containing IOException.
                // In that case we don't want to retry.
                // TODO:JEB resolve this
//                Throwable cause = t;
//                while ((cause = cause.getCause()) != null)
//                {
//                    if (cause instanceof IOException)
//                        throw (IOException) cause;
//                }
//                JVMStabilityInspector.inspectThrowable(t);
                logger.error("failed in streambackground thread", t);
            }
            finally
            {
                ctx.close();

                if (buffers != null)
                    buffers.close();

                //StreamingInboundHandler.this.state = true;
                //FileUtils.closeQuietly(inputStream);
            }
        }
    }
}
