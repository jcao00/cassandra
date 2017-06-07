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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingMessageSender;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Responsible for sending {@link StreamMessage}s to a given peer. We manage an array of netty {@link Channel}s
 * for sending {@link OutgoingFileMessage} instances; all other {@link StreamMessage} types are sent via
 * a special control channel. The reason for this is to treat those messages carefully and not let them get stuck
 * behind a file transfer.
 *
 * One of the challenges when sending files is we might need to delay shipping the file if:
 *
 * - we've exceeded our network I/O use due to rate limiting (at the cassandra level)
 * - the receiver isn't keeping up, which causes the local TCP socket buffer to not empty, which causes epoll writes to not
 * move any bytes to the socket, which causes buffers to stick around in user-land (a/k/a cassandra) memory.
 *
 * When those conditions occur, it's easy enough to reschedule processing the file once the resources pick up
 * (we acquire the permits from the rate limiter, or the socket drains). However, we need to ensure that
 * no other messages are submitted to the same channel while the current file is still being processed.
 */
public class NettyStreamingMessageSender implements StreamingMessageSender
{
    private static final Logger logger = LoggerFactory.getLogger(NettyStreamingMessageSender.class);

    // TODO make me configurable??
    private static final int DEFAULT_MAX_PARALLEL_TRANSFERS = FBUtilities.getAvailableProcessors();

    // a simple mechansim for allowing a degree of fairnes across multiple sessions
    private static final Semaphore fileTransferSemaphore = new Semaphore(DEFAULT_MAX_PARALLEL_TRANSFERS);

    private static final FastThreadLocal<Channel> threadLocalChannel = new FastThreadLocal<>();

    private final StreamSession session;
    private final int protocolVersion;
    private final OutboundConnectionIdentifier connectionId;
    private final StreamConnectionFactory factory;

    private volatile boolean closed;
    private Channel controlMessageChannel;

    // note: this really doesn't need to be a LBQ, just something that's thread safe
    private Collection<ScheduledFuture<?>> channelKeepAlives = new LinkedBlockingQueue<>();

    private final ThreadPoolExecutor fileTransferExecutor;

    public NettyStreamingMessageSender(StreamSession session, OutboundConnectionIdentifier connectionId, StreamConnectionFactory factory, int protocolVersion)
    {
        this.session = session;
        this.protocolVersion = protocolVersion;
        this.connectionId = connectionId;
        this.factory = factory;

        String name = session.peer.toString().replace(':', '.');
        fileTransferExecutor = new DebuggableThreadPoolExecutor(1, DEFAULT_MAX_PARALLEL_TRANSFERS, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                                                new NamedThreadFactory("NettyStreaming-Outbound-" + name));
        fileTransferExecutor.allowCoreThreadTimeOut(true);
    }

    @Override
    public void initialize() throws IOException
    {
        StreamInitMessage message = new StreamInitMessage(FBUtilities.getBroadcastAddress(),
                                                                    session.sessionIndex(),
                                                                    session.planId(),
                                                                    session.description(),
                                                                    session.keepSSTableLevel(),
                                                                    session.isIncremental(),
                                                                    session.getPendingRepair());
        sendMessage(message);
    }

    public boolean hasControlChannel()
    {
        return controlMessageChannel != null;
    }

    public void injectControlMessageChannel(Channel channel)
    {
        this.controlMessageChannel = channel;
    }

    private void setupControlMessageChannel() throws IOException
    {
        if (controlMessageChannel == null)
            controlMessageChannel = createChannel();
    }

    private Channel createChannel() throws IOException
    {
        Channel channel = factory.createConnection(connectionId, protocolVersion);
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(NettyFactory.instance.streamingGroup, NettyFactory.INBOUND_STREAM_HANDLER_NAME, new StreamingInboundHandler(connectionId.remoteAddress(), protocolVersion, session));

        int keepAlivePeriod = DatabaseDescriptor.getStreamingKeepAlivePeriod();
        logger.trace("[Stream #{}] Scheduling keep-alive task with {}s period.", session.planId(), keepAlivePeriod);

        channelKeepAlives.add(channel.eventLoop().scheduleAtFixedRate(new KeepAliveTask(session), keepAlivePeriod * 2, keepAlivePeriod, TimeUnit.SECONDS));
        return channel;
    }

    @Override
    public void sendMessage(StreamMessage message)
    {
        sendMessage(message, future -> onControlMessageComplete(future, message));
    }

    private void sendMessage(StreamMessage message, GenericFutureListener listener)
    {
        if (message instanceof OutgoingFileMessage)
        {
            logger.debug("[Stream #{}] Sending {}", session.planId(), message);
            fileTransferExecutor.submit(new FileStreamTask((OutgoingFileMessage)message));
            return;
        }

        try
        {
            setupControlMessageChannel();
            logger.debug("[Stream #{}] on channel {} Sending {}", session.planId(), controlMessageChannel.id(), message);

            // we anticipate that the control messages are rather small, so allocating a ByteBuf shouldn't  blow out of memory.
            long messageSize = StreamMessage.serializedSize(message, protocolVersion);
            if (messageSize > 1 << 30)
            {
                throw new IllegalStateException(String.format("something is seriously wrong with the calculated stream control message's size: %d bytes, type is %s",
                                                              messageSize, message.type));
            }

            // as control messages are (expected to be) small, we can simply allocate a ByteBuf here, wrap it, and send via the channel
            ByteBuf buf = controlMessageChannel.alloc().directBuffer((int) messageSize, (int) messageSize);
            ByteBuffer nioBuf = buf.nioBuffer(0, (int) messageSize);
            StreamMessage.serialize(message, new DataOutputBufferFixed(nioBuf), protocolVersion, session);
            assert nioBuf.position() == nioBuf.limit();
            buf.writerIndex(nioBuf.position());

            ChannelFuture channelFuture = controlMessageChannel.writeAndFlush(buf);
            channelFuture.addListener(future -> listener.operationComplete(future));
        }
        catch (Exception e)
        {
            close();
            session.onError(e);
        }
    }

    /**
     * Decides what to do after a {@link StreamMessage} is processed.
     *
     * Note: this is called from the netty event loop.
     *
     * @return true if the message was processed sucessfully; else, false.
     */
    private boolean onControlMessageComplete(Future<?> future, StreamMessage msg)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;
        Channel channel = channelFuture.channel();
        Throwable cause = future.cause();
        if (cause == null)
        {
            msg.sent();
            return true;
        }

        logger.error("failed to send a stream message/file to peer {} on channel {}: msg = {}", connectionId, channel.id(), msg, future.cause());

        if (!channel.isOpen())
            session.onError(cause);
        close();

        return false;
    }

    class FileStreamTask implements Runnable
    {
        private final OutgoingFileMessage ofm;

        FileStreamTask(OutgoingFileMessage ofm)
        {
            this.ofm = ofm;
        }

        @Override
        public void run()
        {
            boolean acquiredSemaphore = false;
            try
            {
                while (!(acquiredSemaphore = fileTransferSemaphore.tryAcquire(1, TimeUnit.SECONDS)))
                {
                    if (closed)
                        return;
                }

                Channel channel = getOrCreateChannel();
                // close the DataOutputStreamPlus as we're done with it - but don't close the channel
                try (DataOutputStreamPlus outPlus = ByteBufDataOutputStreamPlus.create(session, channel, 1 << 16))
                {
                    StreamMessage.serialize(ofm, outPlus, protocolVersion, session);
                }
            }
            catch (Exception e)
            {
                session.onError(e);
            }
            finally
            {
                if (acquiredSemaphore)
                    fileTransferSemaphore.release();

                Channel channel = threadLocalChannel.get();
                if (closed && channel != null)
                    channel.close();
            }
        }

        private Channel getOrCreateChannel()
        {
            try
            {
                Channel channel = threadLocalChannel.get();
                if (channel != null)
                    return channel;

                channel = createChannel();
                threadLocalChannel.set(channel);
                return channel;
            }
            catch (Exception e)
            {
                throw new IOError(e);
            }
        }
    }

    /**
     * Periodically sends the {@link KeepAliveMessage}.
     *
     * NOTE: this task, and the callback function {@link #keepAliveListener(Future)} is executes in the netty event loop.
     */
    private class KeepAliveTask implements Runnable
    {
        private final StreamSession session;

        private KeepAliveMessage last = null;

        private KeepAliveTask(StreamSession session)
        {
            this.session = session;
        }

        public void run()
        {
            // if the channel has been closed, or the channel is currently processing a stream transfer,
            // skip this execution.
            if (!controlMessageChannel.isOpen() || closed)
                return;

            UUID planId = session.planId();
            //to avoid jamming the message queue, we only send if the last one was sent
            if (last == null || last.wasSent())
            {
                logger.trace("[Stream #{}] Sending keep-alive to {}.", planId, session.peer);
                last = new KeepAliveMessage();
                sendMessage(last, this::keepAliveListener);
            }
            else
            {
                logger.trace("[Stream #{}] Skip sending keep-alive to {} (previous was not yet sent).", planId, session.peer);
            }
        }

        private void keepAliveListener(Future<? super Void> future)
        {
            if (future.isSuccess() || future.isCancelled())
                return;

            logger.debug("[Stream #{}] Could not send keep-alive message (perhaps stream session is finished?).",
                         session.planId(), future.cause());
        }
    }

    @Override
    public boolean connected()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        logger.debug("[Stream #{}] Closing stream connection channels on {}", session.planId(), session.peer);
        closed = true;
        channelKeepAlives.stream().map(scheduledFuture -> scheduledFuture.cancel(false));
        channelKeepAlives.clear();
        fileTransferExecutor.shutdownNow();

        if (controlMessageChannel != null)
            controlMessageChannel.close();
    }
}
