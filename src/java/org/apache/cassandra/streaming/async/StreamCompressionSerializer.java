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
import org.apache.cassandra.exceptions.ChecksumMismatchException;
import org.apache.cassandra.utils.ChecksumType;

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
     * Length of heaer data, which includes compressed length, uncompressed length, the their chunksum.
     */
    private static final int HEADER_LENGTH = 8;//12;

    private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(HEADER_LENGTH);

    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;
    private final ByteBuffer lengthChecksumBuffer = ByteBuffer.allocate(8);

    public StreamCompressionSerializer(LZ4Compressor compressor, LZ4FastDecompressor decompressor)
    {
        this.compressor = compressor;
        this.decompressor = decompressor;
    }

    public ByteBuf serialize(ByteBuf in, int version) throws IOException
    {
        final int uncompressedLength = in.readableBytes();

        // Note: there's better alternatives to creating extra buffers and memcpy'ing things,
        // but there's a bug in the lz4-java 1.3.0 jar, and it's unmaintained atm :(
        byte[] inBuffer = new byte[uncompressedLength];
        in.readBytes(inBuffer);

        byte[] outBuffer = new byte[compressor.maxCompressedLength(uncompressedLength) + HEADER_LENGTH];
        final int compressedLength = compressor.compress(inBuffer, 0, uncompressedLength, outBuffer, HEADER_LENGTH);

        ByteBuf out = Unpooled.wrappedBuffer(outBuffer, 0, compressedLength + HEADER_LENGTH);
        out.writerIndex(0);
        out.writeInt(compressedLength);
        out.writeInt(uncompressedLength);
        out.writerIndex(HEADER_LENGTH + compressedLength);
        return out;
    }

    // TODO:JEB document that this is a *blocking* implementation
    public ByteBuffer deserialize(ReadableByteChannel in, int version) throws IOException
    {
        headerBuffer.clear();
        int readCount = in.read(headerBuffer);

        // assert readCount = HEADER_LENGTH; ?????

        headerBuffer.flip();
        final int compressedLength = headerBuffer.getInt();
        final int uncompressedLength = headerBuffer.getInt();

        ByteBuffer compressed = ByteBuffer.allocateDirect(compressedLength);
        in.read(compressed);
        compressed.flip();
        ByteBuffer uncompressed = ByteBuffer.allocateDirect(uncompressedLength);
        decompressor.decompress(compressed, uncompressed);
        uncompressed.flip();

        return uncompressed;
    }
}
