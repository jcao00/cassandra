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
import java.nio.channels.ReadableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * A serialiazer for stream compressed files (see package-level documentation). Much like a typical compressed
 * output stream, this class operates on buffers or chunks of the data at a a time. The format for each compressed
 * chunk is as follows:
 *
 * - int - compressed payload length
 * - int - uncompressed payload length
 * - bytes - compressed payload
 */
public class StreamCompressionSerializer
{
    public static final StreamCompressionSerializer serializer = new StreamCompressionSerializer();

    private StreamCompressionSerializer()
    {   }

    /**
     * Length of heaer data, which includes compressed length, uncompressed length.
     */
    private static final int HEADER_LENGTH = 8;

    /**
     * @return A buffer with decompressed data. The returned buffer is taken from the {@link BufferPool}, and
     * thus you need call {@link BufferPool#put(ByteBuffer)} when it has been consumed to ensure the buffer
     * is returned to the pool.
     */
    public ByteBuffer serialize(LZ4Compressor compressor, ByteBuffer in, int version)
    {
        final int uncompressedLength = in.remaining();

        int maxLength = compressor.maxCompressedLength(uncompressedLength);
        ByteBuffer compressed = BufferPool.get(maxLength);
        try
        {
            compressor.compress(in, compressed);
            int compressedLength = compressed.position();
            compressed.limit(compressedLength).position(0);

            ByteBuffer out = BufferPool.get(HEADER_LENGTH + compressedLength);
            out.putInt(compressedLength);
            out.putInt(uncompressedLength);
            out.put(compressed);
            out.flip();
            return out;
        }
        finally
        {
            BufferPool.put(compressed);
        }
    }

    /**
     *
     * @return A buffer with decompressed data. The returned buffer is possibly taken from the {@link BufferPool}, and
     * thus you need call {@link BufferPool#put(ByteBuffer)} when it has been consumed to ensure the buffer
     * is returned to the pool.
     */
    public ByteBuffer deserialize(LZ4FastDecompressor decompressor, DataInputPlus in, int version) throws IOException
    {
        final int compressedLength = in.readInt();
        final int uncompressedLength = in.readInt();

        if (in instanceof ReadableByteChannel)
        {
            ByteBuffer compressed = BufferPool.get(compressedLength);
            ByteBuffer uncompressed = null;
            try
            {
                int readLength = ((ReadableByteChannel) in).read(compressed);
                assert readLength == compressed.position();
                compressed.flip();

                uncompressed = BufferPool.get(uncompressedLength);
                decompressor.decompress(compressed, uncompressed);
                uncompressed.flip();
                FileUtils.clean(compressed);
                return uncompressed;
            }
            catch (Exception e)
            {
                // make sure we return the buffer to the pool on errors
                BufferPool.put(uncompressed);

                if (e instanceof IOException)
                    throw e;
                throw new IOException(e);
            }
            finally
            {
                BufferPool.put(compressed);
            }
        }
        else
        {
            // Note: there's better alternatives to creating extra buffers and memcpy'ing all the things,
            // but there's a bug in the lz4-java 1.3.0 jar wrt using ByteBuffers, and it's unmaintained atm :(
            byte[] compressed = new byte[compressedLength];
            in.readFully(compressed);
            byte[] uncompressed = new byte[uncompressedLength];
            decompressor.decompress(compressed, uncompressed);
            return ByteBuffer.wrap(uncompressed);
        }
    }
}
