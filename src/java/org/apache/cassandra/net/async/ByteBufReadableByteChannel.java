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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelConfig;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ByteBufReadableByteChannel implements ReadableByteChannel
{
    private static final Logger logger = LoggerFactory.getLogger(ByteBufReadableByteChannel.class);

    static final int DISABLED_LOW_WATER_MARK = Integer.MIN_VALUE;
    static final int DISABLED_HIGH_WATER_MARK = Integer.MAX_VALUE;

    private final BlockingQueue<ByteBuf> queue;

    /**
     * The current buffer being read from. Initialize to en empty buffer to avoid needing a null check
     * in every method.
     */
    private ByteBuf buffer;

    /**
     * The count of readable bytes in all {@link ByteBuf}s held by this instance.
     */
    private volatile int queuedByteCount;

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
    public ByteBufReadableByteChannel(ChannelConfig channelConfig)
    {
        this (DISABLED_LOW_WATER_MARK, DISABLED_HIGH_WATER_MARK, channelConfig);
    }

    public ByteBufReadableByteChannel(int lowWaterMark, int highWaterMark, ChannelConfig channelConfig)
    {
        queue = new LinkedBlockingQueue<>();
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.channelConfig = channelConfig;
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
        queuedByteCount += buf.readableBytes();

        if (channelConfig.isAutoRead() && queuedByteCount > highWaterMark)
            channelConfig.setAutoRead(false);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException
    {
        final int len = dst.remaining();
        int remaining = len;
        while (remaining > 0)
        {
            int readableBytes = updateCurrentBuffer();
            int toReadCount = Math.min(remaining, readableBytes);
            dst.limit(dst.position() + toReadCount);
            buffer.readBytes(dst);
            remaining -= toReadCount;
        }
        maybeReleaseBuffer();
        return len;
    }

    /**
     * If the {@link #buffer} is exhausted, release it and try to acquire another from the {@link #queue}.
     * This method will block if the {@link #queue} is empty.
     */
    private int updateCurrentBuffer() throws EOFException
    {
        if (buffer != null)
        {
            int readableBytes = buffer.readableBytes();
            if (readableBytes > 0)
                return readableBytes;

            maybeReleaseBuffer();
        }

        while (true)
        {
            if (closed)
                throw new EOFException();
            try
            {
                buffer = queue.poll(1, TimeUnit.SECONDS);
                if (buffer == null)
                    continue;

                int bytes = buffer.readableBytes();
                queuedByteCount -= bytes;
                bufferInitialReadableBytes = bytes;
                return bytes;
            }
            catch (InterruptedException ie)
            {
                // nop - ignore
            }
        }
    }

    private void maybeReleaseBuffer()
    {
        if (buffer == null || buffer.isReadable())
            return;

        buffer.release();
        buffer = null;
        bufferInitialReadableBytes = 0;

        // decrement the readableByteCount, and possibly re-enable auto-read, *before* blocking on the queue
        // because if we block on the queue without enabling auto-read we'll block forever :(
        if (!channelConfig.isAutoRead() && queuedByteCount < lowWaterMark)
            channelConfig.setAutoRead(true);
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    @Override
    public void close() throws IOException
    {
        if (closed)
            return;
        closed = true;

        if (buffer != null)
        {
            buffer.release(buffer.refCnt());
            buffer = null;
        }

        ByteBuf buf;
        while ((buf = queue.poll()) != null)
            buf.release(buf.refCnt());
    }
}
