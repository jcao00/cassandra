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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.HdrHistogram.Histogram;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.OutboundMessagingConnection.ConnectionType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 *
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because we
 * want to intercept the {@link #channelWritabilityChanged(ChannelHandlerContext)} call.
 */
class MessageOutHandler extends ChannelDuplexHandler // extends MessageToByteEncoder<QueuedMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    public static final int MESSAGE_PREFIX_SIZE = 12;

    private final InetSocketAddress remoteAddr;
    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    private final AtomicLong completedMessageCount;

    private final AtomicLong pendingMessages;
    private final boolean isCoalescing;
    private final boolean captureHistogram;

    private int messageSinceFlush;
    private long lastFlushNanos;

    // TODO:JEB there's metrics capturing code in here that, while handy for short-term perf testing, will need to be removed before commit
    private final Histogram dequeueDelay;
    private final Histogram flushMessagesCount;
    private final Histogram timeBetweenFlushes;
    private final Histogram serializeTime;

    MessageOutHandler(OutboundConnectionParams params)
    {
        this (params.remoteAddr, params.protocolVersion, params.completedMessageCount,
              params.pendingMessageCount, params.coalesce, params.connectionType);
    }

    MessageOutHandler(InetSocketAddress remoteAddr, int targetMessagingVersion, AtomicLong completedMessageCount,
                     AtomicLong pendingMessageCount, boolean isCoalescing, ConnectionType connectionType)
    {
        this.remoteAddr = remoteAddr;
        this.targetMessagingVersion = targetMessagingVersion;
        this.completedMessageCount = completedMessageCount;
        this.pendingMessages = pendingMessageCount;
        this.isCoalescing = isCoalescing;
        captureHistogram = connectionType != ConnectionType.GOSSIP;

        if (captureHistogram)
        {
            dequeueDelay = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
            flushMessagesCount = new Histogram(100_000, 3);
            timeBetweenFlushes = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
            serializeTime = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
        }
        else
        {
            dequeueDelay = null;
            flushMessagesCount = null;
            timeBetweenFlushes = null;
            serializeTime = null;
        }
    }

    private int currentFrameSize;

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        if (captureHistogram)
        {
            lastFlushNanos = System.nanoTime();
            ctx.executor().scheduleWithFixedDelay(() -> log(), 10, 5, TimeUnit.SECONDS);
        }
    }

    private long currentCount;
    public void log()
    {
        try
        {
            long lastCount = currentCount;
            currentCount = dequeueDelay.getTotalCount();
            if (lastCount + 10 > currentCount)
                return;

            logger.info("JEB::DEQUEUE_DELAY: {}", serialize(dequeueDelay));
            logger.info("JEB::MESSAGES_PER_FLUSH: {}", serialize(flushMessagesCount));
            logger.info("JEB::NANOS_BETWEEN_FLUSHES: {}", serialize(timeBetweenFlushes));
            logger.info("JEB::SERIALIZE_TIME: {}", serialize(serializeTime));
        }
        catch (Exception e)
        {
            logger.error("error", e);
        }
    }

    private StringBuilder serialize(Histogram histogram)
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(currentCount).append("::");
        sb.append(histogram.getMinNonZeroValue()).append(',');
        sb.append((long)histogram.getMean()).append(',');
        sb.append(histogram.getValueAtPercentile(75)).append(',');
        sb.append(histogram.getValueAtPercentile(95)).append(',');
        sb.append(histogram.getValueAtPercentile(98)).append(',');
        sb.append(histogram.getValueAtPercentile(99)).append(',');
        sb.append(histogram.getValueAtPercentile(99.9)).append(',');
        sb.append(histogram.getMaxValue());
        return sb;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) throws IOException
    {
        boolean captureHistogram = this.captureHistogram;
        messageSinceFlush++;
        if (message instanceof QueuedMessage)
        {
            long now = System.nanoTime();
            QueuedMessage msg = (QueuedMessage) message;
            if (captureHistogram)
                dequeueDelay.recordValue(System.nanoTime() - msg.timestampNanos());
            captureTracingInfo(msg);
            ByteBuf buf = allocateBuffer(ctx, msg);
            try
            {
                serializeMessage(msg, buf);
                ctx.write(buf, promise);
                if (captureHistogram)
                    serializeTime.recordValue(System.nanoTime() - now);
                // TODO:JEB better error handling here to make sure in the case of failure, we decrement the counts
                completedMessageCount.incrementAndGet();
                if (pendingMessages.decrementAndGet() == 0 && !isCoalescing)
                    flush0(ctx);
            }
            catch (Exception e)
            {
                logger.error("failed to write message to buffer", e);
                if (buf != null && buf.refCnt() > 0)
                    buf.release(buf.refCnt());
            }
        }
        else
        {
            ctx.write(message, promise);
            if (pendingMessages.decrementAndGet() == 0 && !isCoalescing)
                flush0(ctx);
        }
    }

    private ByteBuf allocateBuffer(ChannelHandlerContext ctx, QueuedMessage msg)
    {
        // frame size includes the magic and and other values *before* the actaul serialized message
        currentFrameSize = MESSAGE_PREFIX_SIZE + msg.message.serializedSize(targetMessagingVersion);
        ByteBuf buf = ctx.alloc().ioBuffer(currentFrameSize, currentFrameSize);

        if (buf.maxCapacity() != currentFrameSize || buf.capacity() != currentFrameSize)
            logger.warn("ByteBuf not quite legit: expected size={}, buf={}", currentFrameSize, buf);

        return buf;
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    private void captureTracingInfo(QueuedMessage msg)
    {
        try
        {
            byte[] sessionBytes = msg.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s, size = %d bytes",
                                               msg.message.verb, remoteAddr, msg.message.serializedSize(targetMessagingVersion) + MESSAGE_PREFIX_SIZE);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = msg.message.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    Tracing.instance.trace(ByteBuffer.wrap(sessionBytes), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (msg.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("failed to capture the tracing info for an outbound message, ignoring", e);
        }
    }

    private void serializeMessage(QueuedMessage msg, ByteBuf out) throws IOException
    {
        ByteBufDataOutputPlus bbos = new ByteBufDataOutputPlus(out);
        bbos.writeInt(MessagingService.PROTOCOL_MAGIC);
        bbos.writeInt(msg.id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        bbos.writeInt((int) NanoTimeToCurrentTimeMillis.convert(msg.timestampNanos));
        msg.message.serialize(new WrappedDataOutputStreamPlus(bbos), targetMessagingVersion);

        // next few lines are for debugging ... massively helpful!!
        int spaceRemaining = out.writableBytes();
        if (spaceRemaining != 0)
            logger.error("reported message size {}, actual message size {}, msg {}", out.capacity(), out.writerIndex(), msg.message);
        if (currentFrameSize != out.writerIndex() || currentFrameSize != out.capacity())
            logger.error("currentFrameSize {} is not the same as bytesWritten {}, or buf capacity: {}", currentFrameSize, out.writerIndex(), out.capacity());
    }

    public static class ByteBufDataOutputPlus extends ByteBufOutputStream implements DataOutputPlus
    {
        ByteBufDataOutputPlus(ByteBuf buffer)
        {
            super(buffer);
        }

        @Override
        public void write(ByteBuffer buffer) throws IOException
        {
            buffer().writeBytes(buffer);
        }

        @Override
        public void write(Memory memory, long offset, long length) throws IOException
        {
            for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
                write(buffer);
        }

        @Override
        public <R> R applyToChannel(com.google.common.base.Function<WritableByteChannel, R> c) throws IOException
        {
            // currently, only called in streaming code
            throw new UnsupportedOperationException();
        }
    }

    /**
     * {@inheritDoc}
     *
     * This method will be triggered when a producer thread writes a message the channel, and the size
     * of that message pushes the "open" count of bytes in the channel over the high water mark. The mechanics
     * of netty will wake up the event loop thread (if it's not already executing), trigger the
     * "channelWritabilityChanged" function, which be invoked *after* executing any pending write tasks in the netty queue.
     * Thus, when this method is invoked it's a great time to flush, to push all the written buffers to the kernel
     * for sending.
     *
     * Note: it doesn't matter if coalescing is enabled or disabled, once this function is invoked we want to flush
     * to free up memory.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    {
        if (!ctx.channel().isWritable())
        {
            logger.info("got channelWritabilityChanged, channel size = {} bytes", ctx.channel().unsafe().outboundBuffer().totalPendingWriteBytes());
            flush0(ctx);
        }

        ctx.fireChannelWritabilityChanged();
    }

    private void flush0(ChannelHandlerContext ctx)
    {
        ctx.flush();

        if (captureHistogram)
        {
            flushMessagesCount.recordValue(messageSinceFlush);
            messageSinceFlush = 0;

            long now = System.nanoTime();
            timeBetweenFlushes.recordValue(now - lastFlushNanos);
            lastFlushNanos = now;
        }
    }

    /**
     * {@inheritDoc}
     *
     * When coalese is enabled, we must respect this flush invocation. If coalesce is disabled, we only flush
     * on the conditions stated in the class-level documentation (no more messages in queue, outbound buffer size
     * over the configured limit).
     */
    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        if (isCoalescing)
            flush0(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        flush0(ctx);
    }
}
