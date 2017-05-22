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
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.OutboundConnectionParams;
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
 * the normal internode messaging via {@link MessagingService}. The reason for this is to treat those messages
 * like any other cassandra message type, and only special case the {@link OutgoingFileMessage} as it is
 * the one doing the heavy lifting of transferring sstables. Also, multiple netty channels allow multiple files
 * to be sent in parallel.
 *
 * One of the challenges when sending files is we might need to delay shipping the file if
 *
 * - we've exceeded our network I/O use due to rate limiting (at the cassandra level)
 * - the receiver isn't keeping up, which causes the local socket buffer to not empty, which causes epoll writes to not
 * move any bytes to the socket, which causes buffers to stick around in user-land (a/k/a cassandra) memory.
 *
 * When those conditions occur, it's easy enough to reschedule processing the file once the resouurces pick up
 * (we acquire the permits from the rate limiter, or the socket drains). However, we need to ensure that
 * no other messages are submitted to the same channel while the current file is still being processed.
 *
 * // TODO:JEB cleanup this comment
 * There's also a couple of tricky sizes that should be pointed out:
 * - amount of data in memory (in netty outbound channel) - not affected by zero-copy transfers as nothing comes into user-space
 * - amount of data going out (network bytes)
 * - buffer sizes for reading chunks off disk - sorta not affected by zero-copy
 */
public class NettyStreamingMessageSender implements StreamingMessageSender
{
    private static final Logger logger = LoggerFactory.getLogger(NettyStreamingMessageSender.class);

    /**
     *
     */
    private static final int DEFAULT_CHANNEL_BUFFER_SIZE = 1 << 22;

    // TODO:JEB should this be configurable? also, is this local to this stream session or global to all stream sessions or per-remote peer?
    private static final int DEFAULT_MAX_PARALLEL_TRANSFERS = 4;

    private static final long MAX_WAIT_TIME_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_CONNECT_ATTEMPTS = 3;

    private final StreamSession session;
    private final int protocolVersion;
    private final OutboundConnectionIdentifier connectionId;
    private final ServerEncryptionOptions encryptionOptions;

    private final ExecutorService fileTransferExecutor;

    private Channel controlMessageChannel;

    private volatile boolean closed;


    private static final FastThreadLocal<Channel> threadLocalChannel = new FastThreadLocal<>();

    // TODO:JEB need extra "boolean forceSecure" param to account for behavior in BulkLoadConnFactory
    public NettyStreamingMessageSender(StreamSession session, OutboundConnectionIdentifier connectionId, int protocolVersion)
    {
        this.session = session;
        this.protocolVersion = protocolVersion;
        this.connectionId = connectionId;
//        localAddress = new InetSocketAddress(FBUtilities.getLocalAddress(), 0);
//        boolean secure = MessagingService.isEncryptedConnection(connectionAddress);
//        remoteAddress = new InetSocketAddress(connectionAddress, secure ? DatabaseDescriptor.getSSLStoragePort() : DatabaseDescriptor.getStoragePort());
        encryptionOptions = null;//secure ? DatabaseDescriptor.getServerEncryptionOptions() : null;

        // set size from ctor param
        String name = session.peer.toString().replace(':', '.');
        fileTransferExecutor = new DebuggableThreadPoolExecutor(1, DEFAULT_MAX_PARALLEL_TRANSFERS, 1L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                                                                new NamedThreadFactory("NettyStreaming-Outbound-" + name));
    }

    public boolean hasControlChannel()
    {
        return controlMessageChannel != null;
    }

    public void injectControlMessageChannel(Channel channel)
    {
        this.controlMessageChannel = channel;
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

    private Channel getControlMessageChannel() throws IOException
    {
        if (controlMessageChannel == null)
            controlMessageChannel = createChannel();

        return controlMessageChannel;
    }

    @Override
    public ChannelFuture sendMessage(StreamMessage message) //throws IOException
    {
        if (message instanceof OutgoingFileMessage)
        {
            logger.debug("[Stream #{}] Sending {}", session.planId(), message);
            fileTransferExecutor.submit(new FileStreamTask((OutgoingFileMessage)message));
            // TODO:JEB have better return type - maybe Optional<> ??
            return null;
        }

        // TODO:JEB clean up this error handling - exception *should* percolate up
        try
        {
            if (closed)
                throw new IllegalStateException(String.format("[Stream #%s] not sending outbound message for the stream session " +
                                                              "as the session is closed", session.planId()));

            // TODO:JEB this is fragile, but works for now
            getControlMessageChannel();
            logger.debug("[Stream #{}] on channel {} Sending {}", session.planId(), controlMessageChannel.id(), message);

            // we anticipate that the control messages are rather small, so allocating a ByteBuf shouldn't  blow out of memory.
            long messageSize = StreamMessage.serializedSize(message, protocolVersion);
            if (messageSize > 1 << 30)
            {
                logger.error("something is seriously wrong with the calculated stream control message's size: {} bytes, type is {}", messageSize, message.type);
                return null;
            }

            // TODO:JEB we can fake the funk with control messages as they are small -
            // meaning, we can just allocate a ByteBuf here, wrap it, and send direectly via channel
            ByteBuf buf = controlMessageChannel.alloc().directBuffer((int) messageSize, (int) messageSize);
            ByteBuffer nioBuf = buf.nioBuffer(0, (int) messageSize);
            StreamMessage.serialize(message, new DataOutputBufferFixed(nioBuf), protocolVersion, session);
            assert nioBuf.position() == nioBuf.limit();
            buf.writerIndex(nioBuf.position());
            return controlMessageChannel.writeAndFlush(buf);
        }
        catch (Exception e)
        {
            logger.error("failed to send message: {}", message, e);
        }
        return null;
    }

    class FileStreamTask implements Runnable
    {
        private final OutgoingFileMessage ofm;

        public FileStreamTask(OutgoingFileMessage ofm)
        {
            this.ofm = ofm;
        }

        @Override
        public void run()
        {
            try
            {
                Channel channel = getOrCreateChannel();
                DataOutputStreamPlus outPlus = ByteBufDataOutputStreamPlus.create(channel, 1 << 16);
                StreamMessage.serialize(ofm, outPlus, protocolVersion, session);
            }
            catch (Exception e)
            {
                // TODO:JEB better logging and better error handling
                logger.error("failed to stream file", e);
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
     * Create a new netty {@link Channel} and block for it to connect successfully.
     */
    private Channel createChannel() throws IOException
    {
        // this is the amount of data to allow in memory before netty sets the channel writablility flag to false
        int channelBufferSize = DEFAULT_CHANNEL_BUFFER_SIZE;
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(channelBufferSize >> 2, channelBufferSize);

        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(NettyFactory.Mode.STREAMING)
                                                                  .protocolVersion(protocolVersion)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .waterMark(waterMark)
                                                                  .build();

        Bootstrap bootstrap = NettyFactory.instance.createOutboundBootstrap(params);

        int connectionAttemptCount = 0;
        long now = System.nanoTime();
        final long end = now + MAX_WAIT_TIME_NANOS;
        final Channel channel;
        while (true)
        {
            ChannelFuture channelFuture = bootstrap.connect();
            channelFuture.awaitUninterruptibly(end - now, TimeUnit.MILLISECONDS);
            if (channelFuture.isSuccess())
            {
                channel = channelFuture.channel();
                break;
            }

            connectionAttemptCount++;
            now = System.nanoTime();
            if (connectionAttemptCount == MAX_CONNECT_ATTEMPTS || end - now <= 0)
                throw new IOException("failed to connect to " + connectionId + " for streaming data", channelFuture.cause());
        }

        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(NettyFactory.instance.streamingInboundGroup, NettyFactory.INBOUND_STREAM_HANDLER_NAME, new StreamingInboundHandler(connectionId.remoteAddress(), protocolVersion));

        int keepAlivePeriod = DatabaseDescriptor.getStreamingKeepAlivePeriod();
        logger.trace("[Stream #{}] Scheduling keep-alive task with {}s period.", session.planId(), keepAlivePeriod);

        // TODO:JEB make sure to shut this keep-alive future down
//        ScheduledFuture<?> scheduledFuture = channel.eventLoop().scheduleAtFixedRate(new KeepAliveTask(session), 0, keepAlivePeriod, TimeUnit.SECONDS);

        return channel;
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
            if (!controlMessageChannel.isOpen())
                return;

            UUID planId = session.planId();
            //to avoid jamming the message queue, we only send if the last one was sent
            if (last == null || last.wasSent())
            {
                logger.trace("[Stream #{}] Sending keep-alive to {}.", planId, session.peer);
                last = new KeepAliveMessage();
                sendMessage(last).addListener(future -> keepAliveListener(future));
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

//    /**
//     * Decides what to do after a {@link StreamMessage} is processed.
//     *
//     * Note: this is called from the netty event loop.
//     *
//     * @return true if the message was processed sucessfully; else, false.
//     */
//    private boolean onMessageComplete(Future<? super Void> future, StreamMessage msg)
//    {
//        ChannelFuture channelFuture = (ChannelFuture)future;
//        Channel channel = channelFuture.channel();
//        Throwable cause = future.cause();
//        if (cause == null)
//        {
//            msg.sent();
//            return true;
//        }
//
//        logger.error("failed to send a stream message/file: future = {}, msg = {}", future, msg, future.cause());
//
//        // TODO:JEB double check this correct
//        if (!channel.isOpen())
//            session.onError(cause);
//        close();
//
//        return false;
//    }

    @Override
    public boolean connected()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        logger.debug("[Stream #{}] Closing stream connection channel on {}", session.planId(), session.peer);

        if (!closed)
        {
            closed = true;
            fileTransferExecutor.shutdownNow();
        }
    }
}
