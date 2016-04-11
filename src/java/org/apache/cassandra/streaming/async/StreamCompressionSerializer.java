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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.exceptions.ChecksumMismatchException;
import org.apache.cassandra.net.async.AppendingByteBufInputPlus;
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
class StreamCompressionSerializer
{
    /**
     * Length of heaer data, which includes compressed length, uncompressed length, the their chunksum.
     */
    private static final int HEADER_LENGTH = 12;

    enum DeserializeState { LENGTHS, PAYLOAD }

    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    private DeserializeState deserializeState;
    private ChunkLengths chunkLengths;
    private final ByteBuffer lengthChecksumBuffer = ByteBuffer.allocate(8);

    StreamCompressionSerializer(LZ4Compressor compressor, LZ4FastDecompressor decompressor)
    {
        this.compressor = compressor;
        this.decompressor = decompressor;
        deserializeState = DeserializeState.LENGTHS;
    }

    public ByteBuf serialize(ByteBuf in, int version) throws IOException
    {
        final ChecksumType checksum = ChecksumType.CRC32;
        final int uncompressedLength = in.readableBytes();

        // Note: there's better alternatives to creating extra buffers and memcpy'ing things,
        // but there's a bug in the lz4-java 1.3.0 jar, and it's unmaintained atm :(
        byte[] inBuffer = new byte[uncompressedLength];
        in.readBytes(inBuffer);
        int payloadChecksum = (int)checksum.of(inBuffer, 0, uncompressedLength);

        byte[] outBuffer = new byte[compressor.maxCompressedLength(uncompressedLength) + HEADER_LENGTH + 4];
        final int compressedLength = compressor.compress(inBuffer, 0, uncompressedLength, outBuffer, HEADER_LENGTH);

        ByteBuf out = Unpooled.wrappedBuffer(outBuffer, 0, compressedLength + HEADER_LENGTH + 4);
        out.writerIndex(0);
        out.writeInt(compressedLength);
        out.writeInt(uncompressedLength);
        out.writeInt(checksumLengths(checksum, compressedLength, uncompressedLength));
        out.writerIndex(HEADER_LENGTH + compressedLength);
        out.writeInt(payloadChecksum);
        return out;
    }

    int checksumLengths(ChecksumType checksum, int compressedLength, int uncompressedLength)
    {
        lengthChecksumBuffer.clear();
        lengthChecksumBuffer.putInt(0, compressedLength);
        lengthChecksumBuffer.putInt(4, uncompressedLength);
        return (int) checksum.of(lengthChecksumBuffer);
    }

    public ByteBuf deserialize(AppendingByteBufInputPlus in, int version) throws IOException
    {
        ChecksumType checksum = ChecksumType.CRC32;
        switch (deserializeState)
        {
            case LENGTHS:
                if (in.available() < 12)
                    return null;

                final int compressedLength = in.readInt();
                final int uncompressedLength = in.readInt();
                final int lengthsChecksum = checksumLengths(checksum, compressedLength, uncompressedLength);

                final int checuksumFromStream = in.readInt();
                if (lengthsChecksum != checuksumFromStream)
                    throw new ChecksumMismatchException("unmatched lengths checksums");

                deserializeState = DeserializeState.PAYLOAD;
                // account for the payload and the checksum
                chunkLengths = new ChunkLengths(compressedLength, uncompressedLength);
            // fall-through
            case PAYLOAD:
                if (in.available() < chunkLengths.compressedLength + 4)
                    return null;

                byte[] compressed = new byte[chunkLengths.compressedLength];
                in.readFully(compressed);
                byte[] uncompressed = new byte[chunkLengths.uncompressedLength];
                decompressor.decompress(compressed, uncompressed);

                int payloadChecksum = in.readInt();
                int outputHash = (int)checksum.of(uncompressed, 0, chunkLengths.uncompressedLength);
                if (outputHash != payloadChecksum)
                    throw new ChecksumMismatchException("unmatched payload checksums");

                ByteBuf buf = Unpooled.wrappedBuffer(uncompressed);
                buf.writerIndex(chunkLengths.uncompressedLength);
                deserializeState = DeserializeState.LENGTHS;
                return buf;
            default:
                throw new IllegalArgumentException("Unknown state: " + deserializeState);
        }
    }

    /**
     * For testing only
     */
    DeserializeState getDeserializeState()
    {
        return deserializeState;
    }

    /**
     * A simple struct to hold the lengths of the chunk.
     */
    private static class ChunkLengths
    {
        final int compressedLength;
        final int uncompressedLength;

        ChunkLengths(int compressedLength, int uncompressedLength)
        {
            this.compressedLength = compressedLength;
            this.uncompressedLength = uncompressedLength;
        }
    }
}
