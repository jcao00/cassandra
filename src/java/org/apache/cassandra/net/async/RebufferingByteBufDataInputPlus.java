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
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelConfig;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.io.util.RebufferingInputStream;

public class RebufferingByteBufDataInputPlus extends RebufferingInputStream implements ReadableByteChannel
{
    private static final Logger logger = LoggerFactory.getLogger(RebufferingByteBufDataInputPlus.class);

    static final int DISABLED_LOW_WATER_MARK = Integer.MIN_VALUE;
    static final int DISABLED_HIGH_WATER_MARK = Integer.MAX_VALUE;

    private final BlockingQueue<ByteBuf> queue;

    /**
     * The current buffer being read from. Initialize to en empty buffer to avoid needing a null check
     * in every method.
     */
    private ByteBuf currentBuf;

    /**
     * The count of readable bytes in all {@link ByteBuf}s held by this instance.
     */
    private final AtomicInteger queuedByteCount;

    /**
     * When a {@link ByteBuf} is pulled off the {@link #queue}, capture it's readable byte count before we start reading
     * from it, that we we can properly decrement {@link #queuedByteCount} when we're done with the buffer.
     */
    private int bufferInitialReadableBytes;

    private final int lowWaterMark;
    private final int highWaterMark;
    private final ChannelConfig channelConfig;

    private volatile boolean closed;

    /**
     * Constructs an instance that disables the low and high water mark checks.
     */
    public RebufferingByteBufDataInputPlus(ChannelConfig channelConfig)
    {
        this (DISABLED_LOW_WATER_MARK, DISABLED_HIGH_WATER_MARK, channelConfig);
    }

    public RebufferingByteBufDataInputPlus(int lowWaterMark, int highWaterMark, ChannelConfig channelConfig)
    {
        super(Unpooled.EMPTY_BUFFER.nioBuffer());
        currentBuf = Unpooled.EMPTY_BUFFER;
        queue = new LinkedBlockingQueue<>();
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.channelConfig = channelConfig;

        queuedByteCount = new AtomicInteger();
    }

    // it's expected this method is invoked on the netty event loop
    public void append(ByteBuf buf) throws IllegalStateException
    {
        assert buf != null : "buffer cannot be null";

        if (closed)
        {
            ReferenceCountUtil.release(buf);
            throw new IllegalStateException("stream is already closed, so cannot add another buffer");
        }

        queue.add(buf);
        int queuedCount = queuedByteCount.addAndGet(buf.readableBytes());

        if (channelConfig.isAutoRead() && queuedCount > highWaterMark)
            channelConfig.setAutoRead(false);
    }

    /**
     * {@inheritDoc}
     *
     * Release open buffers and poll the {@link #queue} for more data.
     * <p>
     * This is best, and more or less expected, to be invoked on a consuming thread (not the event loop)
     * becasue if we block on the queue we can't fill it on the event loop (as that's where the buffers are coming from).
     *
     * @throws IOException
     */
    @Override
    protected void reBuffer() throws IOException
    {
        currentBuf.release();
        buffer = null;
        int liveByteCount = queuedByteCount.addAndGet(-bufferInitialReadableBytes);
        bufferInitialReadableBytes = 0;

        // decrement the queuedByteCount, and possibly re-enable auto-read, *before* blocking on the queue
        // because if we block on the queue without enabling auto-read we'll block forever :(
        if (!channelConfig.isAutoRead() && liveByteCount < lowWaterMark)
            channelConfig.setAutoRead(true);

        while (true)
        {
            if (closed)
                throw new ClosedChannelException();
            try
            {
                currentBuf = queue.poll(1, TimeUnit.SECONDS);
                int bytes;
                if (currentBuf == null || (bytes = currentBuf.readableBytes()) == 0)
                    continue;

                buffer = currentBuf.nioBuffer(currentBuf.readerIndex(), bytes);
                assert buffer.remaining() == bytes;
                bufferInitialReadableBytes = bytes;
                return;
            }
            catch (InterruptedException ie)
            {
                // nop - ignore
            }
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        int readLength = dst.remaining();
        int remaining = readLength;

        while (remaining > 0)
        {
            if (!buffer.hasRemaining())
                reBuffer();
            int copyLength = Math.min(remaining, buffer.remaining());

            int originalLimit = buffer.limit();
            buffer.limit(buffer.position() + copyLength);
            dst.put(buffer);
            buffer.limit(originalLimit);
            remaining -= copyLength;
        }

        return readLength;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    /**
     * {@inheritDoc}
     *
     * Note: This should invoked on the consuming thread.
     */
    @Override
    public void close()
    {
        if (closed)
            return;
        closed = true;

        if (currentBuf != null)
        {
            currentBuf.release(currentBuf.refCnt());
            currentBuf = null;
            buffer = null;
        }

        ByteBuf buf;
        while ((buf = queue.poll()) != null)
            buf.release(buf.refCnt());
    }
}
