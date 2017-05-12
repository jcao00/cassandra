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
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * A {@link DataOutputStreamPlus} implementation that is backed by a {@link ByteBuf} that is swapped out with another when full.
 * Each buffer has a {@link ChannelPromise} associated with it,
 * so the downstream progress of buffer (as it is flushed) can be observed. Each message will be serialized into one
 * or more buffers, and each of those buffer's promises are associated with the message. That way the message's promise
 * and be assocaited with the promises of the buffers into which it was serialized. Several messages may be packed into
 * the same buffer, so each of those messages will share the same promise.
 */
public class SwappingByteBufDataOutputStreamPlus extends DataOutputStreamPlus
{
    private static final Logger logger = LoggerFactory.getLogger(SwappingByteBufDataOutputStreamPlus.class);
    private final Channel channel;
    private final int bufferCapacity;

    /**
     * The current {@link ByteBuf} that is being written to.
     */
    private ByteBuf buf;

    public SwappingByteBufDataOutputStreamPlus(Channel channel, int bufferCapacity)
    {
        this.channel = channel;
        this.bufferCapacity = bufferCapacity;
    }

    public Channel channel()
    {
        return channel;
    }

    boolean checkBufSpace(int writeSize)
    {
        if (buf != null)
        {
            if (buf.writableBytes() >= writeSize)
                return true;

            channel.writeAndFlush(buf);
        }

        buf = channel.alloc().directBuffer(bufferCapacity, bufferCapacity);
        return false;
    }

    /**
     * To be used by other methods that write buffers or arrays of bytes to the backing {@link #buf}.
     * Makes sure there's at least *some* writable space in the buf, else we could get into an infinite loop
     * of the buf.writableBytes being 0 and always trying to write 0 bytes to the buf.
     */
    boolean ensureBuffer()
    {
        if (buf != null && buf.isWritable())
            return true;
        checkBufSpace(1);
        return false;
    }

    @Override
    public void flush()
    {
        // only flush() if we have any data to flush
        if (buf != null && buf.isReadable())
        {
            channel.writeAndFlush(buf);
            buf = null;
        }
    }

    @Override
    public void close()
    {
        if (channel.isActive())
        {
            flush();
        }
        else if (buf != null)
        {
            buf.release();
            buf = null;
        }
    }


    @Override
    public void write(int v) throws IOException
    {
        checkBufSpace(Byte.BYTES);
        buf.writeByte((byte) (v & 0xFF));
    }

    @Override
    public void write(byte[] buffer, int offset, int count) throws IOException
    {
        int remaining = count;
        while (remaining > 0)
        {
            ensureBuffer();
            int currentBlockSize = Math.min(buf.writableBytes(), remaining);
            buf.writeBytes(buffer, offset, currentBlockSize);
            offset += currentBlockSize;
            remaining -= currentBlockSize;
        }
    }

    /**
     * {@inheritDoc} - "write the buffer without modifying its position"
     *
     * Unfortunately, netty's {@link ByteBuf#writeBytes(ByteBuffer)} modifies the byteBuffer's position,
     * and that is unsafe in our world wrt multithreading. Hence we need to be careful: reference the backing array
     * on heap ByteBuffers.
     */
    @Override
    public void write(ByteBuffer byteBuffer) throws IOException
    {
        final boolean onHeap = byteBuffer.hasArray();
        int remaining = byteBuffer.remaining();
        int position = byteBuffer.position();
        while (remaining > 0)
        {
            ensureBuffer();
            int nextSize = Math.min(buf.writableBytes(), remaining);

            if (onHeap)
            {
                buf.writeBytes(byteBuffer.array(), byteBuffer.arrayOffset() + position, nextSize);
            }
            else
            {
                ByteBuffer tmp = byteBuffer.duplicate();
                tmp.limit(position + nextSize).position(position);
                buf.writeBytes(tmp);
            }

            position += nextSize;
            remaining -= nextSize;
        }
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> f) throws IOException
    {
        flush();
        return f.apply(new WBC());
    }

    private class WBC implements WritableByteChannel
    {
        public int write(ByteBuffer src) throws IOException
        {
            int size = src.remaining();
            // This is unfortunate: I can't wrap a nio direct buffer with a netty wrapper -
            // the buffer gets corrupted on the transfer. so for now, just copying into a ByteBuf
            ByteBuf buf = channel.alloc().directBuffer(src.remaining(), src.remaining());
            buf.writeBytes(src);
            channel.writeAndFlush(buf);
            return size;
        }

        public boolean isOpen()
        {
            return true;
        }

        public void close() throws IOException
        {

        }
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        checkBufSpace(Byte.BYTES);
        buf.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        checkBufSpace(Byte.BYTES);
        buf.writeByte((byte) (v & 0xFF));
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        checkBufSpace(Short.BYTES);
        buf.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        checkBufSpace(Character.BYTES);
        buf.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        checkBufSpace(Integer.BYTES);
        buf.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        checkBufSpace(Long.BYTES);
        buf.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index) & 0xFF);
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }

    @Override
    public void writeVInt(long v) throws IOException
    {
        writeUnsignedVInt(VIntCoding.encodeZigZag64(v));
    }

    @Override
    public void writeUnsignedVInt(long v) throws IOException
    {
        int size = VIntCoding.computeUnsignedVIntSize(v);
        checkBufSpace(size);
        if (size == 1)
        {
            buf.writeByte((byte) (v & 0xFF));
            return;
        }

        buf.writeBytes(VIntCoding.encodeVInt(v, size), 0, size);
    }
}