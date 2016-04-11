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

package org.apache.cassandra.streaming.async;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ChecksumMismatchException;
import org.apache.cassandra.net.async.AppendingByteBufInputPlus;
import org.apache.cassandra.net.async.AppendingByteBufInputPlusTest.TestChannelConfig;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.streaming.StreamSession;

public class StreamCompressionSerializerTest
{
    private static final int VERSION = StreamSession.CURRENT_VERSION;
    private static final Random random = new Random(2347623847623L);

    private StreamCompressionSerializer serializer;
    private ByteBuf input;
    private ByteBuf compressed;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        serializer = new StreamCompressionSerializer(NettyFactory.lz4Factory().fastCompressor(),
                                                     NettyFactory.lz4Factory().fastDecompressor());
    }

    @After
    public void tearDown()
    {
        if (input != null)
        {
            while (input.refCnt() > 0)
                input.release();
        }

        if (compressed != null)
        {
            while (compressed.refCnt() > 0)
                compressed.release();
        }
    }

    @Test
    public void roundTrip_HappyPath() throws IOException
    {
        int bufSize = 1 << 14;
        input = PooledByteBufAllocator.DEFAULT.buffer(bufSize, bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.writeInt(random.nextInt());

        compressed = serializer.serialize(input, VERSION);
        AppendingByteBufInputPlus inputPlus = new AppendingByteBufInputPlus(new TestChannelConfig());
        inputPlus.append(compressed);
        Assert.assertEquals(StreamCompressionSerializer.DeserializeState.LENGTHS, serializer.getDeserializeState());
        ByteBuf output = serializer.deserialize(inputPlus, VERSION);

        input.readerIndex(0);
        Assert.assertEquals(input, output);
        // make sure that after consuming the entire chunk, we reset the state
        Assert.assertEquals(StreamCompressionSerializer.DeserializeState.LENGTHS, serializer.getDeserializeState());
    }

    @Test (expected = ChecksumMismatchException.class)
    public void roundTrip_CorruptLength() throws IOException
    {
        int bufSize = 1 << 14;
        input = PooledByteBufAllocator.DEFAULT.buffer(bufSize, bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.writeInt(random.nextInt());

        compressed = serializer.serialize(input, VERSION);
        // fip a bit in the an arbitrary middle byte of the length
        int b = compressed.getByte(1);
        b = (b & 0x01) == 0 ? b + 1 : b - 1;
        compressed.setByte(1, (byte) b);

        AppendingByteBufInputPlus inputPlus = new AppendingByteBufInputPlus(new TestChannelConfig());
        inputPlus.append(compressed);
        serializer.deserialize(inputPlus, VERSION);
    }

    @Test (expected = ChecksumMismatchException.class)
    public void roundTrip_CorruptPayload() throws IOException
    {
        int bufSize = 1 << 14;
        input = PooledByteBufAllocator.DEFAULT.buffer(bufSize, bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.writeInt(random.nextInt());

        compressed = serializer.serialize(input, VERSION);
        int positionToCorrupt = compressed.writerIndex() - 10;
        compressed.setByte(positionToCorrupt, (byte)(compressed.getByte(positionToCorrupt) << 1));

        AppendingByteBufInputPlus inputPlus = new AppendingByteBufInputPlus(new TestChannelConfig());
        inputPlus.append(compressed);
        serializer.deserialize(inputPlus, VERSION);
    }
}
