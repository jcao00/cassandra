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
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.io.util.DataInputPlus;

/**
 * A serialiazer for stream compressed files (see package-level documentation). Much like a typical compressed
 * output stream, this class operates on buffers or chunks of the data at a a time. The format for each compressed
 * chunk is as follows:
 *
 * - int - compressed payload length
 * - int - uncompressed payload length
 * - int - checksum of lengths
 * - bytes - compressed payload
 * - int - checksum of uncompressed data
 *
 * Note: instances are not thread-safe.
 */
public class StreamCompressionSerializer
{
    /**
     * Length of heaer data, which includes compressed length, uncompressed length.
     */
    private static final int HEADER_LENGTH = 8;

    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    public StreamCompressionSerializer(LZ4Compressor compressor, LZ4FastDecompressor decompressor)
    {
        this.compressor = compressor;
        this.decompressor = decompressor;
    }

    public ByteBuf serialize(ByteBuffer in, int version) throws IOException
    {
        final int uncompressedLength = in.remaining();

        // Note: there's better alternatives to creating extra buffers and memcpy'ing all the things,
        // but there's a bug in the lz4-java 1.3.0 jar wrt using ByteBuffers, and it's unmaintained atm :(
        final byte[] inBuffer;
        final int inOffset;
        if (in.hasArray())
        {
            inBuffer = in.array();
            inOffset = in.arrayOffset();
        }
        else
        {
            inBuffer = new byte[uncompressedLength];
            in.get(inBuffer);
            inOffset = 0;
        }

        byte[] outBuffer = new byte[compressor.maxCompressedLength(uncompressedLength) + HEADER_LENGTH];
        final int compressedLength = compressor.compress(inBuffer, 0, uncompressedLength, outBuffer, HEADER_LENGTH);

        ByteBuf out = Unpooled.wrappedBuffer(outBuffer, inOffset, compressedLength + HEADER_LENGTH);
        out.writerIndex(0);
        out.writeInt(compressedLength);
        out.writeInt(uncompressedLength);
        out.writerIndex(HEADER_LENGTH + compressedLength);
        return out;
    }

    // TODO:JEB document that this is a *blocking* implementation
    public ByteBuffer deserialize(DataInputPlus in, int version) throws IOException
    {
        final int compressedLength = in.readInt();
        final int uncompressedLength = in.readInt();

        if (in instanceof ReadableByteChannel)
        {
            ByteBuffer compressed = ByteBuffer.allocateDirect(compressedLength);
            int readLength = ((ReadableByteChannel)in).read(compressed);
            assert readLength == compressed.position();
            compressed.flip();

            ByteBuffer uncompressed = ByteBuffer.allocateDirect(uncompressedLength);
            decompressor.decompress(compressed, uncompressed);
            uncompressed.flip();
            return uncompressed;
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
