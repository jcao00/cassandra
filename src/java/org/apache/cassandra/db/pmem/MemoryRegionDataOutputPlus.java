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

package org.apache.cassandra.db.pmem;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Function;

import lib.llpl.MemoryRegion;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;

public class MemoryRegionDataOutputPlus implements DataOutputPlus
{
    private final MemoryRegion region;
    private final long size;
    private int position;

    public MemoryRegionDataOutputPlus(MemoryRegion region, int initialPosition)
    {
        this.region = region;
        size = region.size();
        position = initialPosition;
    }

    public boolean hasPosition()
    {
        return true;
    }

    public long position()
    {
        return position;
    }

    public void position(int position)
    {
        this.position = position;
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        int startBufPosition = buffer.position();
        region.getBuffer(position, buffer);
        position += buffer.position() - startBufPosition;
    }

    @Override
    public void write(Memory memory, long offset, long length) throws IOException
    {
        for (ByteBuffer buffer : memory.asByteBuffers(offset, length))
            write(buffer);
    }

    @Override
    public <R> R applyToChannel(Function<WritableByteChannel, R> c)
    {
        return null;
    }

    @Override
    public void write(int b) throws IOException
    {
        region.putByte(position, (byte) b);
        position++;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        region.copyFromArray(b, 0, position, b.length);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        region.copyFromArray(b, off, position, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        region.putByte(position, v ? (byte) 1 : (byte) 0);
        position++;
    }

    @Override
    public void writeByte(int v)
    {
        region.putByte(position, (byte) v);
        position++;
    }

    @Override
    public void writeShort(int v)
    {
        region.putShort(position, (short) v);
        position += 2;
    }

    @Override
    public void writeChar(int v)
    {
        // TODO might need a putChar method on MemeoryRegion
        region.putShort(position, (char) v);
        position += 2;
    }

    @Override
    public void writeInt(int v)
    {
        region.putInt(position, v);
        position += 4;
    }

    @Override
    public void writeLong(long v)
    {
        region.putLong(position, v);
        position += 8;
    }

    @Override
    public void writeFloat(float v)
    {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v)
    {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public void writeBytes(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index));
    }

    @Override
    public void writeChars(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        UnbufferedDataOutputStreamPlus.writeUTF(s, this);
    }
}
