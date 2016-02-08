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

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Flushes messages at appropriate time.
 * <p>
 * When we flush depends on whether message coalescing is enabled or not.
 * <p>
 * If coalescing isn't enabled, we simply flush on every message.
 * <p>
 * If coalescing is enabled, how often we flush is delegated to the coalescing strategy. More precisely, when a new
 * message arrives:
 * <ul>
 *   <li>either a flush has been scheduled (by a previous message) already, and we just wait on that</li>
 *   <li>or no flush is scheduled and we ask the coalescing strategy when to schedule a flush</li>
 * </ul>
 * <p>
 */
class FlushHandler extends ChannelOutboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(FlushHandler.class);

    private final CoalescingStrategy coalescingStrategy;

    // The currently scheduled flush, or null.
    private ScheduledFuture<?> scheduledFlush;

    private boolean closed;

    FlushHandler(CoalescingStrategy coalescingStrategy)
    {
        this.coalescingStrategy = coalescingStrategy;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        if (closed)
        {
            promise.setFailure(new ClosedChannelException());
            return;
        }

        ctx.write(msg, promise);
        if (!coalescingStrategy.isCoalescing())
        {
            assert scheduledFlush == null;
            ctx.flush();
            return;
        }

        coalescingStrategy.newArrival((QueuedMessage)msg);

        if (scheduledFlush != null)
            return;

        long flushDelayNanos = coalescingStrategy.currentCoalescingTimeNanos();
        if (flushDelayNanos <= 0)
        {
            ctx.flush();
            return;
        }

        scheduledFlush = ctx.executor().schedule(() -> {
            scheduledFlush = null;
            ctx.flush();
        }, flushDelayNanos, TimeUnit.NANOSECONDS);
    }

    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        // nop - all flush behavior is expected to be controlled by the write() method
    }

    @VisibleForTesting
    void setClosed()
    {
        closed = true;
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        closed = true;
        // Grabs reference to make sure we don't race with the task execution (which sets scheduledFlush to null)
        ScheduledFuture<?> registeredFlush = scheduledFlush;
        if (registeredFlush != null)
        {
            ctx.flush();
            registeredFlush.cancel(false);
            scheduledFlush = null;
        }

        ctx.close(promise);
    }
}
