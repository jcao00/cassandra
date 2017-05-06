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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelConfig;
import io.netty.util.ReferenceCountUtil;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.io.util.DataInputPlus;

/**
 * An {@link InputStream} that blocks on a {@link #queue} for {@link ByteBuf}s. An instance is responsibile for the reference
 * counting of any {@link ByteBuf}s passed to {@link #append(ByteBuf)}.
 *
 * {@link #maybeReleaseBuffer()} needs to be invoked at the end of (almost) all the {@link java.io.DataInput} methods to:
 * 1) release buffers as soon as they are empty (as a general hygene detail)
 * 2) more importantly, possibly update the auto-read status (to enabled) so we can continue reading buffers from the socket.
 *
 * Instances are thread-safe only to the extent of expecting a single consumer, although there may be multiple producers.
 * The producer side is expected to largely only call {@link #append(ByteBuf)}, while consumers/readers call all the
 * other methods. Because of this, we put all the auto-read handling logic into the (singlt-threaded) consumer to prevent
 * crazy-pants race conditions from occuring.
 *
 */
public class AppendingByteBufInputPlus implements DataInputPlus, Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(AppendingByteBufInputPlus.class);

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
    public AppendingByteBufInputPlus(ChannelConfig channelConfig)
    {
        this (DISABLED_LOW_WATER_MARK, DISABLED_HIGH_WATER_MARK, channelConfig);
    }

    public AppendingByteBufInputPlus(int lowWaterMark, int highWaterMark, ChannelConfig channelConfig)
    {
        queue = new LinkedBlockingQueue<>();
        this.lowWaterMark = lowWaterMark;
        this.highWaterMark = highWaterMark;
        this.channelConfig = channelConfig;
    }

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
                buffer = queue.take();
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
        if (buffer == null)
            return;

        if (buffer.isReadable())
        {
            // i don't like calling memoryConsumed() on the hot path (on every read method invocation)
            // but updating the auto-read on the event loop is a race with the deserialize thread.
            // however, calling it every time, rather than waiting until the current buffer is exhausted, allows us to
            // more quickly limit the amount of memory used by unconsumed streaming buffers.
            if (channelConfig.isAutoRead() && memoryConsumed() > highWaterMark)
                channelConfig.setAutoRead(false);
            return;
        }

        buffer.release();
        buffer = null;
        bufferInitialReadableBytes = 0;

        // decrement the readableByteCount, and possibly re-enable auto-read, *before* blocking on the queue
        // because if we block on the queue without enabling auto-read we'll block forever :(
        if (!channelConfig.isAutoRead() && memoryConsumed() < lowWaterMark)
            channelConfig.setAutoRead(true);
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] out, int off, int len) throws IOException
    {
        while (len > 0)
        {
            int readableBytes = updateCurrentBuffer();
            int toReadCount = Math.min(len, readableBytes);
            buffer.readBytes(out, off, toReadCount);
            len -= toReadCount;
            off += toReadCount;
        }
        maybeReleaseBuffer();
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        updateCurrentBuffer();
        final byte b = buffer.readByte();
        maybeReleaseBuffer();
        return b;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return readByte() & 0xff;
    }

    @DontInline
    private long readPrimitiveSlowly(int bytes) throws IOException
    {
        long result = 0;
        for (int i = 0; i < bytes; i++)
            result = (result << 8) | (readByte() & 0xFFL);
        return result;
    }

    @Override
    public short readShort() throws IOException
    {
        final short s;
        if (updateCurrentBuffer() >= 2)
            s = buffer.readShort();
        else
            s = (short) readPrimitiveSlowly(2);

        maybeReleaseBuffer();
        return s;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException
    {
        final char c;
        if (updateCurrentBuffer() >= 2)
            c = buffer.readChar();
        else
            c = (char) readPrimitiveSlowly(2);

        maybeReleaseBuffer();
        return c;
    }

    @Override
    public int readInt() throws IOException
    {
        final int i;
        if (updateCurrentBuffer() >= 4)
            i = buffer.readInt();
        else
            i = (int) readPrimitiveSlowly(4);

        maybeReleaseBuffer();
        return i;
    }

    @Override
    public long readLong() throws IOException
    {
        final long l;
        if (updateCurrentBuffer() >= 8)
            l = buffer.readLong();
        else
            l = readPrimitiveSlowly(8);

        maybeReleaseBuffer();
        return l;
    }

    @Override
    public float readFloat() throws IOException
    {
        final float f;
        if (updateCurrentBuffer() >= 4)
            f = buffer.readFloat();
        else
            f = Float.intBitsToFloat((int) readPrimitiveSlowly(4));

        maybeReleaseBuffer();
        return f;
    }

    @Override
    public double readDouble() throws IOException
    {
        final double d;
        if (updateCurrentBuffer() >= 8)
            d = buffer.readDouble();
        else
            d = Double.longBitsToDouble(readPrimitiveSlowly(8));

        maybeReleaseBuffer();
        return d;
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * Each of the read* methods invoked will perform the {@link #maybeReleaseBuffer()} call.
     */
    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    /**
     * The count of bytes used in memory by all of the current buffers in {@link #queue} and {@link #buffer}.
     */
    private int memoryConsumed()
    {
        return queuedByteCount + (buffer != null ? bufferInitialReadableBytes : 0);
    }

    /**
     * The count of bytes remaining to be consumed from all of the current buffers in {@link #queue} and {@link #buffer}.
     */
    public int available()
    {
        return queuedByteCount + (buffer != null ? buffer.readableBytes() : 0);
    }

    @Override
    public void close()
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

    public void read(ByteBuffer dst) throws IOException
    {
        if (dst.hasArray())
        {
            readFully(dst.array(), dst.arrayOffset(), dst.remaining());
        }
        else
        {
            int len = dst.remaining();
            while (len > 0)
            {
                int readableBytes = updateCurrentBuffer();
                int toReadCount = Math.min(len, readableBytes);
                buffer.readBytes(dst);
                len -= toReadCount;
            }
            maybeReleaseBuffer();
        }
        dst.position(dst.limit());
    }
}
