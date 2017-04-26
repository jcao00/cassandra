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

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Comparator;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.OutboundConnectionParams;
import org.apache.cassandra.streaming.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingMessageSender;
import org.apache.cassandra.streaming.messages.KeepAliveMessage;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

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
 * Hence, the use of an internal {@link #queue} to hold pending outbound messages. When a {@link OutgoingFileMessage} is
 * streamed to a peer, it's promise will be fulfilled, and listeners invoked. We attach
 * {@link #onMessageComplete(Future, StreamMessage)} as a promise listener, and on success of a message
 * the {@link #queue} is checked for more messages to send (on the same channel).
 *
 * We use a netty channel {@link Attribute}, specifically {@link #CHANNEL_BUSY_ATTR} to indicate if a channel
 * is currently processing a file transfer. While a channel attribute is not the highest performing, our needs
 * for performance wrt a flag indicating a busy state are pretty low, so the attribute is fine.
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
    private static final int DEFAULT_CHANNEL_BUFFER_SIZE = 1 << 17;

    /**
     * A netty-specific attribute that we assign to each {@link Channel} instance that is used to indicate
     * if the channel is currently processing a message (transferring a sstable).
     */
    private static final AttributeKey<Boolean> CHANNEL_BUSY_ATTR = AttributeKey.newInstance("channelBusy");

    // TODO:JEB should this be configurable? also, is this local to this stream session or global to all stream sessions or per-remote peer?
    private static final int DEFAULT_MAX_PARALLEL_TRANSFERS = 4;

    private final StreamSession session;
    private final int protocolVersion;
    private final OutboundConnectionIdentifier connectionId;
    private final ServerEncryptionOptions encryptionOptions;

    /**
     * A queue to use for backlogging messages is there is already a message being processed.
     */
    private final Queue<StreamMessage> queue;

    private final AtomicReferenceArray<Channel> transferChannels;
    private volatile boolean closed;

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
        transferChannels = new AtomicReferenceArray<>(new Channel[DEFAULT_MAX_PARALLEL_TRANSFERS]);
        queue = new PriorityBlockingQueue<>(8, MessageComparator.INSTANCE);
    }

    /**
     * Note: this comparator imposes orderings that are inconsistent with equals().
     */
    private static class MessageComparator implements Comparator<StreamMessage>
    {
        private static final MessageComparator INSTANCE = new MessageComparator();

        @Override
        public int compare(StreamMessage message1, StreamMessage message2)
        {
            if (message1.getType().isTransfer())
                return -1;
            if (message2.getType().isTransfer())
                return 1;
            return 0;
        }
    }

    /**
     * Submit a {@link StreamMessage} to be sent to the peer via a netty channel. If there is a slot open in
     * {@link #transferChannels}, create a new {@link Channel} and publish the message to it; else, enqueue the message
     * for future consumption.
     */
    @Override
    public void sendMessage(StreamMessage message)
    {
        logger.debug("[Stream #{}] Sending {}", session.planId(), message);
        if (closed)
            throw new IllegalStateException(String.format("[Stream #%s] not sending outbound message for the stream session " +
                                                          "as the session is closed", session.planId()));

        queue.offer(message);
        Channel channel = getNewTransferChannel();
        if (channel != null)
            tryPublishFromQueue(channel);
    }

    /**
     * Get a new or not currently busy netty {@link Channel} if we haven't hit the max count. Given how the streaming
     * functionality works, there should not be much contention, if any, on creating a channel. Thus, it's probably
     * acceptable to create then destroy an excess connection/channel.
     */
    private Channel getNewTransferChannel()
    {
        int channelCount = transferChannels.length();
        for (int i = 0; i < channelCount; i++)
        {
            Channel channel = transferChannels.get(i);
            if (channel != null && channel.isOpen())
            {
                Attribute<Boolean> busy = channel.attr(CHANNEL_BUSY_ATTR);
                if (busy.compareAndSet(false, true))
                    return channel;
            }
            else
            {
                // TODO:JEB just logging for now on connect fail, but not sure this is the correct long-term behavior
                try
                {
                    channel = createChannel();
                }
                catch (IOException e)
                {
                    logger.warn("failed to create outbound streaming connection to {}", connectionId, e);
                    return null;
                }

                channel.attr(CHANNEL_BUSY_ATTR).set(false);

                // we went to the effort of creating the channel, so try to put that channel
                // in any remaining, available slot.
                while (i < channelCount)
                {
                    if (transferChannels.compareAndSet(i, null, channel))
                        return channel;
                    i++;
                }

                // TODO:JEB be careful as there might be legacy code on the receiver to handle the closing of a connection
                // which is treated as a failed/closed session
                logger.debug("lost the race to use a streaming channel (another channel took the slot)");
                channel.close();
            }
        }
        return null;
    }

    private final long MAX_WAIT_TIME_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_CONNECT_ATTEMPTS = 3;

    /**
     * Create a new netty {@link Channel} and block for it to connect successfully.
     */
    private Channel createChannel() throws IOException
    {
        // this is the amount of data to allow in memory before netty sends the channgeWritablityChanged event
        int channelBufferSize = DEFAULT_CHANNEL_BUFFER_SIZE;
        WriteBufferWaterMark waterMark = new WriteBufferWaterMark(channelBufferSize >> 1, channelBufferSize);

        int sendBufferSize = DatabaseDescriptor.getInternodeSendBufferSize() > 0
                             ? DatabaseDescriptor.getInternodeSendBufferSize()
                             : OutboundConnectionParams.DEFAULT_SEND_BUFFER_SIZE;

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(connectionId)
                                                                  .encryptionOptions(encryptionOptions)
                                                                  .mode(NettyFactory.Mode.STREAMING)
                                                                  .protocolVersion(StreamMessage.CURRENT_VERSION)
                                                                  .sendBufferSize(sendBufferSize)
                                                                  .waterMark(waterMark)
                                                                  .build();

        Bootstrap bootstrap = NettyFactory.instance.createOutboundBootstrap(params, false);

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

        channel.writeAndFlush(new FirstHandshakeMessage(protocolVersion, NettyFactory.Mode.STREAMING, false));
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(NettyFactory.instance.streamingInboundGroup, NettyFactory.INBOUND_STREAM_HANDLER_NAME, new StreamingInboundHandler(connectionId.remoteAddress(), protocolVersion));
        pipeline.addLast(NettyFactory.OUTBOUND_STREAM_HANDLER_NAME, new StreamingOutboundHandler(session, protocolVersion, StreamRateLimiter.instance));

        int keepAlivePeriod = DatabaseDescriptor.getStreamingKeepAlivePeriod();
        logger.trace("[Stream #{}] Scheduling keep-alive task with {}s period.", session.planId(), keepAlivePeriod);
        channel.eventLoop().scheduleAtFixedRate(new KeepAliveTask(channel), 0, keepAlivePeriod, TimeUnit.SECONDS);

        return channel;
    }

    private class KeepAliveTask implements Runnable
    {
        private final Channel channel;

        private KeepAliveMessage last = null;

        private KeepAliveTask(Channel channel)
        {
            this.channel = channel;
        }

        public void run()
        {
            // if the channel has been closed, or the channel is currently processing a stream transfer,
            // skip this execution.
            if (!channel.isOpen() || !channel.attr(CHANNEL_BUSY_ATTR).get())
                return;

            UUID planId = session.planId();
            //to avoid jamming the message queue, we only send if the last one was sent
            if (last == null || last.wasSent())
            {
                logger.trace("[Stream #{}] Sending keep-alive to {}.", planId, session.peer);
                last = new KeepAliveMessage(planId, session.sessionIndex());
                channel.writeAndFlush(last).addListener(future -> keepAliveListener(future));
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


    /**
     * Attempts to pull a message off the {@link #queue} and publish it to the provided netty channel.
     * Each channel should only process (transfer) one file at a time, so we need careful handling of how messages
     * get published to each channel. (see the class-level javadoc about how file transfer can be paused.)
     *
     * This method handles the tricky race conditions that could arise between threads
     * adding new messages to the queue versus the netty event loop thread that executes
     * {@code {@link #onMessageComplete(Future, StreamMessage)}} upon completing the previous message.
     *
     * We need to be careful about concurrency as we get here from several paths:
     *
     * 1) {@link StreamSession#startStreamingFiles()}, which is only called once in the life of the session (at the beginning),
     * 2) {@link #onMessageComplete(Future, StreamMessage)}, the callback from the netty event loop
     *
     * @return true if a message was sent to the netty channel; else, false.
     */
    private boolean tryPublishFromQueue(Channel channel)
    {
        if (!queue.isEmpty() && channel.attr(CHANNEL_BUSY_ATTR).compareAndSet(false, true))
        {
            StreamMessage msg = queue.poll();
            if (msg != null)
            {
                ChannelFuture future = channel.writeAndFlush(msg);
                future.addListener(f -> onMessageComplete(f, msg));
                return true;
            }
            // we won the race to mark this channel as busy (and the queue was not empty when we first checked),
            // but now the queue is empty (we lost the race to get a message), so set the channel back to not busy.
            channel.attr(CHANNEL_BUSY_ATTR).set(false);

            // Note: it's arguable that we could close the connection here if there's no work remaining to do, but we don't know the
            // array index into {@link #transferChannels). further, an idle socket consumes virtually zero resources, and becasue
            // it's a netty channel it's not necessarily a distinct thread - tl;dr skip the complexity of closing the channel here
            // as we'll close all the channels at the end of the session.
        }
        return false;
    }

    /**
     * Decides what to do after a {@link StreamMessage} is processed.
     *
     * Note: this is called from the netty event loop.
     *
     * @return true if the message was processed sucessfully; else, false.
     */
    private boolean onMessageComplete(Future<? super Void> future, StreamMessage msg)
    {
        ChannelFuture channelFuture = (ChannelFuture)future;
        Channel channel = channelFuture.channel();
        Throwable cause = future.cause();
        if (cause != null)
        {
            msg.sent();
            channel.attr(CHANNEL_BUSY_ATTR).set(false);
            tryPublishFromQueue(channel);
            return true;
        }

        logger.error("failed to stream a file: future = {}, cause = {}", future, future.cause());

        // TODO:JEB double check this correct
        if (!channel.isOpen())
            session.onError(cause);
        close();

        return false;
    }

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
            queue.clear();
            for (int i = 0; i < transferChannels.length(); i++)
            {
                Channel channel = transferChannels.get(i);
                if (channel != null)
                    channel.close();

                transferChannels.set(i, null);
            }
        }
    }
}
