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
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.jctools.queues.SpscArrayQueue;

/**
 * An {@link InputStream} that blocks on a {@link #queue} for {@link ByteBuf}s. An instance is responsibile for the reference
 * counting of any {@link ByteBuf}s passed to {@link #append(ByteBuf)}.
 *
 * Note: Instances are thread-safe only to the extent of expecting a single producer and single consumer.
 */
public class AppendingByteBufInputStream extends InputStream
{
    private static final Logger logger = LoggerFactory.getLogger(AppendingByteBufInputStream.class);

    private static final int DISABLED_WATER_MARK = Integer.MIN_VALUE;
    private final byte[] oneByteArray = new byte[1];

    private final SpscArrayQueue<ByteBuf> queue;
    private final int lowWaterMark;
    private final int highWaterMark;
    private final ChannelHandlerContext ctx;

    private ByteBuf currentBuf;
    private volatile boolean closed;

    // count of readable bytes -- needed to know if a sufficient number of bytes available for a given read

    /**
     * Total count of bytes in all {@link ByteBuf}s held by this instance. This is retained so that we know when to enable/disable
     * the netty channel's auto-read behavior. This value only indicates that total number of bytes in all buffers, and does
     * not distinguish between read and unread bytes - just anything taking up memory.
     */
    private final AtomicInteger liveByteCount;

    /**
     * The count of readable bytes in all {@link ByteBuf}s held by this instance.
     */
    private final AtomicInteger readableByteCount;


    public AppendingByteBufInputStream(ChannelHandlerContext ctx)
    {
        this(DISABLED_WATER_MARK, DISABLED_WATER_MARK, ctx);
    }

    public AppendingByteBufInputStream(int lowWaterMark, int highWaterMark, ChannelHandlerContext ctx)
    {
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.ctx = ctx;
        queue = new SpscArrayQueue<>(1 << 10);
        liveByteCount = new AtomicInteger(0);
        readableByteCount = new AtomicInteger(0);
    }

    public void append(ByteBuf buf) throws IllegalStateException
    {
        if (closed)
        {
            ReferenceCountUtil.release(buf);
            throw new IllegalStateException("stream is already closed, so cannot add another buffer");
        }
//        logger.debug("**** buffer append readable bytes = {}, capacity = {}", buf.readableBytes(), buf.capacity() );
        readableByteCount.addAndGet(buf.readableBytes());
        updateBufferedByteCount(buf.capacity());
        queue.add(buf);
    }

    void updateBufferedByteCount(int diff)
    {
        int liveCount = liveByteCount.addAndGet(diff);

        if (highWaterMark != DISABLED_WATER_MARK)
        {
            ChannelConfig config = ctx.channel().config();
            boolean autoRead = config.isAutoRead();
            // TODO:JEB damnit, this is a data race! fix me
            if (liveCount < lowWaterMark && !autoRead)
            {
//                logger.info("enabling autoRead");
                config.setAutoRead(true);
            }
            else if (liveCount > highWaterMark && autoRead)
            {
//                logger.info("disabling autoRead");
                config.setAutoRead(false);
            }
        }
    }

    @Override
    public int read() throws IOException
    {
        int result = read(oneByteArray, 0, 1);
        if (result == 1)
            return oneByteArray[0] & 0xFF;
        if (result == -1)
            return -1;

        throw new IOException("failed to read from stream");
    }

    public int read(byte out[], int off, final int len) throws IOException
    {
        if (out == null)
            throw new NullPointerException();
        else if (off < 0 || len < 0 || len > out.length - off)
            throw new IndexOutOfBoundsException();
        else if (len == 0)
            return 0;

        int remaining = len;
        while (true)
        {
            if (currentBuf != null)
            {
                if (currentBuf.isReadable())
                {
                    int toReadCount = Math.min(remaining, currentBuf.readableBytes());
                    currentBuf.readBytes(out, off, toReadCount);
                    remaining -= toReadCount;
                    readableByteCount.addAndGet(-toReadCount);

                    if (remaining == 0)
                        return len;
                    off += toReadCount;
                }

                updateBufferedByteCount(-currentBuf.capacity());
                currentBuf.release();
                currentBuf = null;
            }

            // the jctools queues are non-blocking, so if the queue is empty, we need some kind of wait mechanism.
            // we could try a spinlock, but as we're waiting for network data (packet arrival, kernel handling of TCP),
            // it's not as predictable as just waiting for the computation from another CPU or thread.
            // thus a spinlock may just end up hogging the CPU, which is probably not what we want for a background activity like streaming.
            while ((currentBuf = queue.poll()) == null)
            {
                Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MICROSECONDS);
                if (Thread.interrupted() || closed)
                    throw new EOFException();
            }
//            logger.info("got buf from queue, readable byte = {}", currentBuf.readableBytes());
        }
    }

    public int readableBytes()
    {
        return readableByteCount.get();
    }

    @VisibleForTesting
    public int buffersInQueue()
    {
        return queue.size();
    }

    // TODO:JEB this isn't quite thread safe ... don't do anything stoopid.
    // current expected use is only on the producer thread, with no contention. Thus, it needs to be externally synchronized.
    // Does *not* call BB#release() as we're essentially transferring ownership of the bufs to the caller.
    public int drain(final long maxBytes, Consumer<ByteBuf> consumer)
    {
        int readBytes = 0;  // the count of readableBytes consumed
        int removedBufSizes = 0; // the total count of bytes from buffers that have been consumed
        try
        {
            if (currentBuf != null)
            {
                if (currentBuf.readableBytes() > maxBytes)
                {
                    ByteBuf buf = currentBuf.readRetainedSlice((int)maxBytes);
                    consumer.accept(buf);
                    readBytes = (int)maxBytes;
                    return readBytes;
                }

                readBytes = currentBuf.readableBytes();
                removedBufSizes = currentBuf.capacity();
                consumer.accept(currentBuf);
                currentBuf = null;
            }

            if (readBytes < maxBytes)
            {
                ByteBuf buf;
                // only peek() the queue as we don't know if we should remove the buffer yet
                while ((buf = queue.peek()) != null)
                {
                    long remaining = maxBytes - readBytes;
                    if (buf.readableBytes() > remaining)
                    {
                        ByteBuf b = buf.readRetainedSlice((int) remaining);
                        consumer.accept(b);
                        readBytes += remaining;
                        return (int)maxBytes;
                    }

                    // we know we'll consume the entire buffer, so go ahead and pull it off the queue
                    buf = queue.poll();
                    readBytes += buf.readableBytes();
                    removedBufSizes += buf.capacity();
                    consumer.accept(buf);
                }
            }

            return readBytes;
        }
        finally
        {
//            logger.debug("at end of drain() 1, drainCount = {}, readableByteCount = {}, liveByteCount = {}", drainCount, readableByteCount, liveByteCount);
            if (readBytes > 0)
                readableByteCount.addAndGet(-readBytes);
            if (removedBufSizes > 0)
                updateBufferedByteCount(-removedBufSizes);
//            logger.debug("at end of drain() 2, drainCount = {}, readableByteCount = {}, liveByteCount = {}", drainCount, readableByteCount, liveByteCount);
//            drainCount++;
        }
    }

    @Override
    public void close()
    {
        if (closed)
            return;
        closed = true;

        if (currentBuf != null)
        {
            while (currentBuf.refCnt() > 0)
                currentBuf.release();
            currentBuf = null;
        }

        ByteBuf buf;
        while ((buf = queue.poll()) != null)
            buf.release();
    }
}
