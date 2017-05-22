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

package org.apache.cassandra.streaming.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputStreamPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;

/**
 * The intent of this class is to only be used in a very narrow use-case: on the stream compression path of streaming.
 * This class should really only get calls to {@link #write(ByteBuffer)}, where the incoming buffer is compressed and sent
 * downstream.
 */
public class ByteBufCompressionDataOutputStreamPlus extends WrappedDataOutputStreamPlus
{
    private final StreamRateLimiter limiter;
    private final StreamCompressionSerializer compressionSerializer;

    public ByteBufCompressionDataOutputStreamPlus(DataOutputStreamPlus out, StreamRateLimiter limiter)
    {
        super(out);
        assert out instanceof ByteBufDataOutputStreamPlus;
        compressionSerializer = new StreamCompressionSerializer(NettyFactory.lz4Factory().fastCompressor(),
                                                                NettyFactory.lz4Factory().fastDecompressor());
        this.limiter = limiter;
    }

    @Override
    public void write(ByteBuffer buffer) throws IOException
    {
        ByteBuf compressed = compressionSerializer.serialize(buffer, StreamSession.CURRENT_VERSION);

        // this is a blocking call - you have been warned
        limiter.acquire(compressed.readableBytes());

        ((ByteBufDataOutputStreamPlus)out).write(compressed);
    }
}
