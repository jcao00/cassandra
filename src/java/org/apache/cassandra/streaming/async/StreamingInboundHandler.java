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
import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.net.async.ByteBufReadableByteChannel;
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

    public static final int CHECKSUM_LENGTH = Integer.BYTES;

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 15; // 1 << 19 = 512Kb
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 16; // 1 << 22 = 4Mb

    enum State { START, MESSAGE_PAYLOAD, FILE_TRANSFER_HEADER_PAYLOAD, FILE_TRANSFER_PAYLOAD, CLOSED }

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    private State state;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     */
    private ByteBufReadableByteChannel bufChannel;

    /**
     * A background thread that performs the deserialization of the sstable data.
     */
    private Thread blockingIOThread;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        state = State.START;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        bufChannel = new ByteBufReadableByteChannel(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx.channel().config());
        blockingIOThread = new FastThreadLocalThread(new DeserializingSstableTask(ctx),
                                                     String.format("Stream-Inbound--%s-%s", ctx.channel().id(), remoteAddress.toString()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception
    {
        if (state == State.CLOSED)
        {
            ReferenceCountUtil.release(message);
            return;
        }

        if (!(message instanceof ByteBuf))
        {
            ctx.fireChannelRead(message);
            return;
        }

        bufChannel.append((ByteBuf) message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    private void close()
    {
        if (state == State.CLOSED)
            return;

        state = State.CLOSED;
        blockingIOThread.interrupt();
        // TODO:JEB release resources;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing streaming file", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

    /**
     * For testing
     */
    State getState()
    {
        return state;
    }

    /**
     * For testing only!!
     */
    void setPendingBuffers(ByteBufReadableByteChannel bufChannel)
    {
        this.bufChannel = bufChannel;
    }

    /**
     */
    private class DeserializingSstableTask implements Runnable
    {
        private final ChannelHandlerContext ctx;

        private StreamSession session;

        DeserializingSstableTask(ChannelHandlerContext ctx)
        {
            this.ctx = ctx;
        }

        @Override
        public void run()
        {
            try
            {
                while (state != State.CLOSED)
                {
                    StreamMessage message = StreamMessage.deserialize(bufChannel, protocolVersion, null);
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
                        // TODO: i'd be great to check if the session actually exists before slurping in the entire sstable
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
//            catch (InterruptedException e)
//            {
//                // nop, thread was interrupted by the parent class (this is, or should be, normal/good/happy path)
//            }
            catch (EOFException e)
            {
                // thrown when netty socket closes/is interrupted
                logger.debug("eof reading from socket; closing");
            }
            catch (Throwable t)
            {
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

                // TODO:JEB do we close the session or send complete somewheres?
                ctx.close();

                //StreamingInboundHandler.this.state = true;
                //FileUtils.closeQuietly(inputStream);
            }
        }
    }
}
