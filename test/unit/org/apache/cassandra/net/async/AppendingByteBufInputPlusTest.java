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
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelOption;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.UnbufferedDataOutputStreamPlus;

import static org.apache.cassandra.net.async.AppendingByteBufInputPlus.DISABLED_HIGH_WATER_MARK;
import static org.apache.cassandra.net.async.AppendingByteBufInputPlus.DISABLED_LOW_WATER_MARK;

public class AppendingByteBufInputPlusTest
{
    private AppendingByteBufInputPlus inputPlus;

    private ByteBuf buf1;
    private ByteBuf buf2;
    private TestChannelConfig channelConfig;

    @Before
    public void setUp()
    {
        channelConfig = new TestChannelConfig();
        inputPlus = new AppendingByteBufInputPlus(DISABLED_LOW_WATER_MARK, DISABLED_HIGH_WATER_MARK, channelConfig);
    }

    @After
    public void tearDown()
    {
        inputPlus.close();

        if (buf1 != null)
        {
            while (buf1.refCnt() > 0)
                buf1.release();
        }

        if (buf2 != null)
        {
            while (buf2.refCnt() > 0)
                buf2.readableBytes();
        }
    }

    @Test
    public void append()
    {
        int size = 400;
        buf1 = Unpooled.buffer(size);
        buf1.writerIndex(size);
        inputPlus.append(buf1);
        Assert.assertEquals(size, inputPlus.available());
    }

    @Test (expected = IllegalStateException.class)
    public void append_Closed()
    {
        inputPlus.close();
        buf1 = Unpooled.buffer(1);
        inputPlus.append(buf1);
    }

    @Test
    public void testAutoRead() throws IOException
    {
        int lowWaterMark = 2;
        int highWaterMark = 4;
        inputPlus = new AppendingByteBufInputPlus(lowWaterMark, highWaterMark, channelConfig);
        Assert.assertTrue(channelConfig.isAutoRead());

        int size = highWaterMark * 2;
        buf1 = Unpooled.buffer(size);
        buf1.writerIndex(size);
        inputPlus.append(buf1);
        Assert.assertTrue(channelConfig.isAutoRead());

        inputPlus.readInt();
        Assert.assertFalse(channelConfig.isAutoRead());
        inputPlus.readInt();
        Assert.assertTrue(channelConfig.isAutoRead());
    }

    @Test
    public void readShort_SingleBuffer() throws IOException
    {
        short val = 42;
        buf1 = Unpooled.buffer(4);
        buf1.writeShort(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readShort());
    }

    @Test
    public void readShort_TwoBuffers() throws IOException
    {
        short val = Short.MAX_VALUE - 17;
        buf1 = Unpooled.buffer(1);
        buf1.writeByte(val >> 8);
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(2);
        buf2.writeByte((byte)val);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readShort());
    }

    @Test
    public void readChar_SingleBuffer() throws IOException
    {
        char val = 'a';
        buf1 = Unpooled.buffer(4);
        buf1.writeChar(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readChar());
    }

    @Test
    public void readChar_TwoBuffers() throws IOException
    {
        char val = Character.MAX_VALUE - 1034;
        buf1 = Unpooled.buffer(1);
        buf1.writeByte(val >> 8);
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(2);
        buf2.writeByte((byte)val);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readChar());
    }

    @Test
    public void readInt_SingleBuffer() throws IOException
    {
        int val = 42;
        buf1 = Unpooled.buffer(4);
        buf1.writeInt(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readInt());
    }

    @Test
    public void readInt_TwoBuffers() throws IOException
    {
        int val = 934982734;
        buf1 = Unpooled.buffer(2);
        buf1.writeShort(val >> 16);
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(2);
        buf2.writeShort((short)val);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readInt());
    }

    @Test
    public void readLong_SingleBuffer() throws IOException
    {
        long val = 289372387463L;
        buf1 = Unpooled.buffer(8);
        buf1.writeLong(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readLong());
    }

    @Test
    public void readLong_TwoBuffers() throws IOException
    {
        long val = -1239487345976344L;
        buf1 = Unpooled.buffer(4);
        buf1.writeInt((int)(val >> 32));
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(4);
        buf2.writeInt((int)val);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readLong());
    }

    @Test
    public void readFloat_SingleBuffer() throws IOException
    {
        float val = 2347.23476423F;
        buf1 = Unpooled.buffer(4);
        buf1.writeFloat(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readFloat(), 0);
    }

    @Test
    public void readFloat_TwoBuffers() throws IOException
    {
        float val = -2342347.23476423F;
        int intBits = Float.floatToIntBits(val);
        buf1 = Unpooled.buffer(2);
        buf1.writeShort(intBits >> 16);
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(2);
        buf2.writeShort((short)intBits);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readFloat(), 0);
    }

    @Test
    public void readDouble_SingleBuffer() throws IOException
    {
        double val = 42342374623460.9234253472342D;
        buf1 = Unpooled.buffer(4);
        buf1.writeDouble(val);
        inputPlus.append(buf1);
        Assert.assertEquals(val, inputPlus.readDouble(), 0);
    }

    @Test
    public void readDouble_TwoBuffers() throws IOException
    {
        double val = 42342374623460.9234253472342D;
        long longBits = Double.doubleToLongBits(val);
        buf1 = Unpooled.buffer(4);
        buf1.writeInt((int)(longBits >> 32));
        inputPlus.append(buf1);
        buf2 = Unpooled.buffer(2);
        buf2.writeInt((int)longBits);
        inputPlus.append(buf2);
        Assert.assertEquals(val, inputPlus.readDouble(), 0);
    }

    @Test
    public void readFully() throws IOException
    {
        int size = 256;
        byte[] array = new byte[size];
        for (int i = 0; i < size; i++)
            array[i] = (byte)i;
        buf1 = Unpooled.wrappedBuffer(array);
        inputPlus.append(buf1);

        byte[] out = new byte[size];
        inputPlus.readFully(out);
        Assert.assertArrayEquals(array, out);
    }

    @Test (expected = UnsupportedOperationException.class)
    public void readLine() throws IOException
    {
        inputPlus.readLine();
    }

    @Test
    public void readUTF() throws IOException
    {
        String s = "hello, world!";
        DataOutputBuffer out = new DataOutputBuffer(128);
        UnbufferedDataOutputStreamPlus.writeUTF(s, out);
        buf1 = Unpooled.wrappedBuffer(out.toByteArray(), 0, out.getLength());
        inputPlus.append(buf1);
        Assert.assertEquals(s, inputPlus.readUTF());
    }

    @Test (expected = UnsupportedOperationException.class)
    public void skipBytes() throws IOException
    {
        inputPlus.skipBytes(0);
    }

    @Test
    public void close()
    {
        buf1 = Unpooled.buffer(8, 8);
        buf1.writerIndex(5);
        buf2 = Unpooled.buffer(8, 8);
        buf2.writerIndex(3);
        Assert.assertEquals(1, buf1.refCnt());
        Assert.assertEquals(1, buf2.refCnt());

        inputPlus.append(buf1);
        inputPlus.append(buf2);
        inputPlus.close();

        Assert.assertEquals(0, buf1.refCnt());
        Assert.assertEquals(0, buf2.refCnt());
    }

    public static class TestChannelConfig implements ChannelConfig
    {
        boolean autoRead = true;

        public Map<ChannelOption<?>, Object> getOptions()
        {
            return null;
        }

        public boolean setOptions(Map<ChannelOption<?>, ?> options)
        {
            return false;
        }

        public <T> T getOption(ChannelOption<T> option)
        {
            return null;
        }

        public <T> boolean setOption(ChannelOption<T> option, T value)
        {
            return false;
        }

        public int getConnectTimeoutMillis()
        {
            return 0;
        }

        public ChannelConfig setConnectTimeoutMillis(int connectTimeoutMillis)
        {
            return null;
        }

        public int getMaxMessagesPerRead()
        {
            return 0;
        }

        public ChannelConfig setMaxMessagesPerRead(int maxMessagesPerRead)
        {
            return null;
        }

        public int getWriteSpinCount()
        {
            return 0;
        }

        public ChannelConfig setWriteSpinCount(int writeSpinCount)
        {
            return null;
        }

        public ByteBufAllocator getAllocator()
        {
            return null;
        }

        public ChannelConfig setAllocator(ByteBufAllocator allocator)
        {
            return null;
        }

        public <T extends RecvByteBufAllocator> T getRecvByteBufAllocator()
        {
            return null;
        }

        public ChannelConfig setRecvByteBufAllocator(RecvByteBufAllocator allocator)
        {
            return null;
        }

        public boolean isAutoRead()
        {
            return autoRead;
        }

        public ChannelConfig setAutoRead(boolean autoRead)
        {
            this.autoRead = autoRead;
            return null;
        }

        public boolean isAutoClose()
        {
            return false;
        }

        public ChannelConfig setAutoClose(boolean autoClose)
        {
            return null;
        }

        public int getWriteBufferHighWaterMark()
        {
            return 0;
        }

        public ChannelConfig setWriteBufferHighWaterMark(int writeBufferHighWaterMark)
        {
            return null;
        }

        public int getWriteBufferLowWaterMark()
        {
            return 0;
        }

        public ChannelConfig setWriteBufferLowWaterMark(int writeBufferLowWaterMark)
        {
            return null;
        }

        public MessageSizeEstimator getMessageSizeEstimator()
        {
            return null;
        }

        public ChannelConfig setMessageSizeEstimator(MessageSizeEstimator estimator)
        {
            return null;
        }

        public WriteBufferWaterMark getWriteBufferWaterMark()
        {
            return null;
        }

        public ChannelConfig setWriteBufferWaterMark(WriteBufferWaterMark writeBufferWaterMark)
        {
            return null;
        }
    }
}
