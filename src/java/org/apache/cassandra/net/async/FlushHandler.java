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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * TODO:JEB document the flush story: flush when no more messages are following currnt message,
 * and when the current bytes in channel > 64k (or whatever size).
 *
 *
 * TODO:JEB figure out the story of where exactly this handle, wrt channelWritabilityChanged
 * Note: this handler must come *after* the messages have been serialized.
 */
public class FlushHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(FlushHandler.class);

    private final AtomicLong pendingMessages;

    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging-FLUSH");
    private static final Histogram messagesSinceFlushHisto = Metrics.histogram(factory.createMetricName("MOH-MessagesSinceFlush"), false);
    private static final Histogram bytesSinceFlushHisto = Metrics.histogram(factory.createMetricName("MOH-BytesSinceFlush"), false);
    private static final Timer flushDelayNanosHisto = Metrics.timer(factory.createMetricName("MOH-FlushDelayLatency"));

    private int messagesSinceFlush;
    private int bytesSinceFlush;
    private long lastFlushNanos;

    static
    {
        //startTimerDump();
    }

    private static void startTimerDump()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(new TimerDumper(), 1, 1, TimeUnit.SECONDS);
    }

    private static class TimerDumper implements Runnable
        {
        long currentCount;

        public void run()
        {
            try
            {
                Snapshot snapshot = messagesSinceFlushHisto.getSnapshot();

                long lastCount = currentCount;
                currentCount = messagesSinceFlushHisto.getCount();
                if (lastCount + 10 > currentCount)
                    return;

                logger.info("JEB::MESSAGES_SINCE_FLUSH: {}", serialize(snapshot));
                logger.info("JEB::BYTES_SINCE_FLUSH: {}", serialize(bytesSinceFlushHisto.getSnapshot()));
                logger.info("JEB::FLUSH_DELAY_NANOS: {}", serialize(flushDelayNanosHisto.getSnapshot()));
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

    public FlushHandler(OutboundConnectionParams updatedParams)
    {
        this.pendingMessages = updatedParams.pendingMessageCount;
    }

    /**
     * {@inheritDoc}
     *
     * We want to flush if there are no other more messages in the netty channel.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    {
        ctx.write(msg, promise);

        messagesSinceFlush++;
        bytesSinceFlush += ((ByteBuf)msg).readableBytes();

        if (pendingMessages.decrementAndGet() == 0)
            flushInternal(ctx);
    }

    private void flushInternal(ChannelHandlerContext ctx)
    {
        ctx.flush();
//        messagesSinceFlushHisto.update(messagesSinceFlush);
//        messagesSinceFlush = 0;
//        bytesSinceFlushHisto.update(bytesSinceFlush);
//        bytesSinceFlush = 0;
//        long now = System.nanoTime();
//
//        if (lastFlushNanos > 0)
//            flushDelayNanosHisto.update(now - lastFlushNanos, TimeUnit.NANOSECONDS);
//
//        lastFlushNanos = now;
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
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    {
        if (!ctx.channel().isWritable())
        {
            logger.info("got channelWritabilityChanged event, gonna flush()");
            flushInternal(ctx);
        }

        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        // TODO:JEB ignoring these calls for now
//        flushInternal(ctx);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        flushInternal(ctx);
    }
}
