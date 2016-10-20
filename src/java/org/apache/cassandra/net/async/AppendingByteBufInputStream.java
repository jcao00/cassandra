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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.service.StorageService;
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
    private final byte[] oneByteArray = new byte[1];
    private final SpscArrayQueue<ByteBuf> queue;
    private final AtomicInteger bufferredByteCount;
    private final int lowWaterMark;
    private final int highWaterMark;
    private final ChannelHandlerContext ctx;

    private ByteBuf currentBuf;
    private int currentBufSize;
    private volatile boolean closed;

    public AppendingByteBufInputStream(int lowWaterMark, int highWaterMark, ChannelHandlerContext ctx)
    {
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.ctx = ctx;
        queue = new SpscArrayQueue<>(1 << 10);
        bufferredByteCount = new AtomicInteger(0);
    }

    public void append(ByteBuf buf) throws IllegalStateException
    {
        if (closed)
        {
            ReferenceCountUtil.release(buf);
            throw new IllegalStateException("stream is already closed, so cannot add another buffer");
        }
        updateBufferedByteCount(buf.readableBytes());
        queue.add(buf);
    }

    void updateBufferedByteCount(int diff)
    {
        while (true)
        {
            int cur = bufferredByteCount.intValue();
            if (bufferredByteCount.compareAndSet(cur, cur + diff))
                break;
        }

        // TODO:JEB damnit, this is a data race! fix me
        int val = bufferredByteCount.get();
        if (val < lowWaterMark)
            ctx.channel().config().setAutoRead(true);
        else if (val > highWaterMark)
            ctx.channel().config().setAutoRead(false);
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

                    if (remaining == 0)
                        return len;
                    off += toReadCount;
                }

                updateBufferedByteCount(-currentBufSize);
                currentBuf.release();
                currentBuf = null;
                currentBufSize = 0;
            }

            // the jctools queues are non-blocking, so if the queue is empty, we need some kind of wait mechanism.
            // we could try a spinlock, but as we're waiting for network data (packet arrival, kernel handling of TCP),
            // it's not as predictable as just waiting for the computation from another CPU or thread.
            // thus a spinlock may just end up hogging the CPU, which is probably not what we want for a background activity like streaming.
            while ((currentBuf = queue.poll()) == null)
            {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MICROSECONDS);
                if (Thread.interrupted() || closed)
                    throw new EOFException();
            }
            logger.info("got buf from queue, readable byte = {}", currentBuf.readableBytes());
            currentBufSize = currentBuf.readableBytes();
        }
    }

    public int getBufferredByteCount()
    {
        return bufferredByteCount.get();
    }

    @VisibleForTesting
    public int buffersInQueue()
    {
        return queue.size();
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
