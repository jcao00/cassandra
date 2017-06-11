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
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.messages.StreamMessage;

public class StreamCompressionSerializerTest
{
    private static final int VERSION = StreamMessage.CURRENT_VERSION;
    private static final Random random = new Random(2347623847623L);

    private final StreamCompressionSerializer serializer = StreamCompressionSerializer.serializer;
    private final LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    private final LZ4FastDecompressor decompressor = LZ4Factory.fastestInstance().fastDecompressor();

    private ByteBuffer input;
    private ByteBuffer compressed;
    private ByteBuffer output;

    @After
    public void tearDown()
    {
        if (input != null)
            FileUtils.clean(input);
        if (compressed != null)
            FileUtils.clean(compressed);
        if (output != null)
            FileUtils.clean(output);
    }

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void roundTrip_HappyPath() throws IOException
    {
        int bufSize = 1 << 14;
        input = ByteBuffer.allocateDirect(bufSize);
        for (int i = 0; i < bufSize; i += 4)
            input.putInt(random.nextInt());
        input.flip();

        compressed = serializer.serialize(compressor, input, VERSION);
        input.flip();
        output = serializer.deserialize(decompressor, new DataInputBuffer(compressed, false), VERSION);

        Assert.assertEquals(input.remaining(), output.remaining());
        for (int i = 0; i < input.remaining(); i++)
            Assert.assertEquals(input.get(i), output.get(i));
        FileUtils.clean(output);
    }
}
