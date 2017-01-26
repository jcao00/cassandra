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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 */
class MessageOutHandler extends MessageToByteEncoder<QueuedMessage>
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

    // TODO:JEB there's metrics capturing code in here that, while handy for short-term perf testing, will need to be removed before commit
    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");
    private static final Timer serializationDelay = Metrics.timer(factory.createMetricName("MOH-SerializationLatency"));

    static
    {
        startTimerDump();
    }

    private static void startTimerDump()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(new TimerDumper(), 1, 1, TimeUnit.SECONDS);
    }

    static class TimerDumper implements Runnable
    {
        long currentCount;

        public void run()
        {
            try
            {
                Snapshot snapshot = serializationDelay.getSnapshot();

                long lastCount = currentCount;
                currentCount = serializationDelay.getCount();
                if (lastCount + 10 > currentCount)
                    return;

                logger.info("JEB::SERAILIZATION_DELAY: {}", serialize(snapshot));
            }
            catch (Exception e)
            {
                logger.error("error", e);
            }
        }

        private StringBuilder serialize(Snapshot snapshot)
        {
            StringBuilder sb = new StringBuilder(256);
            sb.append(currentCount).append("::");
            sb.append(snapshot.getMin()).append(',');
            sb.append((long)snapshot.getMedian()).append(',');
            sb.append((long)snapshot.get75thPercentile()).append(',');
            sb.append((long)snapshot.get95thPercentile()).append(',');
            sb.append((long)snapshot.get98thPercentile()).append(',');
            sb.append((long)snapshot.get99thPercentile()).append(',');
            sb.append((long)snapshot.get999thPercentile()).append(',');
            sb.append(snapshot.getMax());
            return sb;
        }
    }

    MessageOutHandler(OutboundConnectionParams params)
    {
        this (params.remoteAddr, params.protocolVersion, params.completedMessageCount);
    }

    MessageOutHandler(InetSocketAddress remoteAddr, int targetMessagingVersion, AtomicLong completedMessageCount)
    {
        this.remoteAddr = remoteAddr;
        this.targetMessagingVersion = targetMessagingVersion;
        this.completedMessageCount = completedMessageCount;
    }


    private int currentFrameSize;

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, QueuedMessage msg, boolean preferDirect) throws Exception
    {
        long now = System.nanoTime();
        serializationDelay.update(now - msg.timestampNanos(), TimeUnit.NANOSECONDS);

        // frame size includes the magic and and other values *before* the actaul serialized message
        currentFrameSize = MESSAGE_PREFIX_SIZE + msg.message.serializedSize(targetMessagingVersion);

        ByteBuf buf;
        if (preferDirect)
            buf = ctx.alloc().ioBuffer(currentFrameSize, currentFrameSize);
        else
            buf = ctx.alloc().heapBuffer(currentFrameSize, currentFrameSize);

        if (buf.maxCapacity() != currentFrameSize || buf.capacity() != currentFrameSize)
            logger.warn("ByteBuf not quite legit: expected size={}, buf={}", currentFrameSize, buf);

        return buf;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, QueuedMessage msg, ByteBuf out) throws IOException
    {
        captureTracingInfo(msg);
        serializeMessage(msg, out);
        completedMessageCount.incrementAndGet();
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

    public class ByteBufDataOutputPlus extends ByteBufOutputStream implements DataOutputPlus
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


}
