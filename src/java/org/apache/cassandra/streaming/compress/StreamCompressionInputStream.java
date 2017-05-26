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

import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

public class StreamCompressionInputStream extends RebufferingInputStream
{
    /**
     * The stream which contains buffers of compressed data that came from the peer.
     */
    private final DataInputPlus dataInputPlus;

    private final LZ4FastDecompressor decompressor;
    private final int protocolVersion;

    public StreamCompressionInputStream(DataInputPlus dataInputPlus, int protocolVersion)
    {
        super(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        this.dataInputPlus = dataInputPlus;
        this.protocolVersion = protocolVersion;
        this.decompressor = NettyFactory.lz4Factory().fastDecompressor();
    }

    @Override
    public void reBuffer() throws IOException
    {
        // release the current backing buffer
        BufferPool.put(buffer);

        buffer = StreamCompressionSerializer.deserialize(decompressor, dataInputPlus, protocolVersion);
    }

    @Override
    public void close()
    {
        BufferPool.put(buffer);
    }
}
