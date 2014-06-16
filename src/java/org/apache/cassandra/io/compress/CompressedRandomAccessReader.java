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
package org.apache.cassandra.io.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.BufferProvider;
import org.apache.cassandra.io.util.CompressedPoolingSegmentedFile;
import org.apache.cassandra.io.util.PoolingSegmentedFile;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader extends RandomAccessReader
{
    public static CompressedRandomAccessReader open(String path, CompressionMetadata metadata, CompressedPoolingSegmentedFile owner)
    {
        try
        {
            return new CompressedRandomAccessReader(path, metadata, owner);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompressedRandomAccessReader open(String dataFilePath, CompressionMetadata metadata)
    {
        try
        {
            return new CompressedRandomAccessReader(dataFilePath, metadata, null);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Checksum checksum;

    // raw checksum bytes
//    private final ByteBuffer checksumBytes = channel.allocateBuffer(4);

    protected CompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, PoolingSegmentedFile owner) throws FileNotFoundException
    {
        super(new File(dataFilePath), metadata.chunkLength(), owner);
        this.metadata = metadata;
        checksum = metadata.hasPostCompressionAdlerChecksums ? new Adler32() : new CRC32();
        compressed = channel.allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));
    }

    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        assert Integer.bitCount(bufferSize) == 1;
        return BufferProvider.NioBufferProvider.INSTANCE.allocateBuffer(bufferSize);
    }

    @Override
    protected void reBuffer()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            if (channel.position() != chunk.offset)
                channel.position(chunk.offset);

            boolean mustReadChecksum = readChecksumBytes();
            int readLen = chunk.length + (mustReadChecksum ? 4 : 0);

            if (compressed.capacity() < readLen)
            {
                channel.destroyByteBuffer(compressed);
                compressed = channel.allocateBuffer(readLen);
            }
            else
              compressed.limit(readLen);

            int readCnt = channel.read(compressed);
            if (readCnt < readLen)
                throw new CorruptBlockException(getPath(), chunk);

            // technically flip() is unnecessary since all the remaining work uses the raw array, but if that changes
            // in the future this will save a lot of hair-pulling
            compressed.flip();
            buffer.clear();
            int decompressedBytes;
            byte[] onHeapCompressed;
            try
            {
                // need this on heap as DirectBB doesn't support array()
                // interestingly, nio impl (FileChannel.read()) does more or less the same thing - create a tmp direct buffer
                // for the data from the file, then copy into an on-heap buffer for app use.
                onHeapCompressed = getBytes(compressed, chunk.length);
                decompressedBytes = metadata.compressor().uncompress(onHeapCompressed, 0, chunk.length, buffer.array(), 0);
//                buffer.limit(decompressedBytes);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }

            if (mustReadChecksum)
            {
                if (metadata.hasPostCompressionAdlerChecksums)
                {
//                    byte[] onHeapCompressed = getBytes(compressed, chunk.length);
                    checksum.update(onHeapCompressed, 0, chunk.length);
                }
                else
                {
                    checksum.update(buffer.array(), 0, decompressedBytes);
                }

                if (compressed.getInt(chunk.length) != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum ob( back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            bufferOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - bufferOffset));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    boolean readChecksumBytes()
    {
        return metadata.parameters.getCrcCheckChance() > FBUtilities.threadLocalRandom().nextDouble();
    }

    // need to copy on-heap if the buffer is a DirectBB ... <sigh>
    private byte[] getBytes(ByteBuffer buffer, int length)
    {
        if (buffer.isDirect())
        {
            //i think we need to reposition (to get back to beginning of buffer)????
            buffer.position(0);
            byte[] b = new byte[length];
            buffer.get(b);
            return b;
        }
        return buffer.array();
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
//        assert channel.position() == chunk.offset + chunk.length ;

        //TODO: fix this broken hack - basically, it would be cool to use channel.clearByteBuffer(),
        // but it kept the requestedSze for the compressed buffer, and when we force that as the limit
        // for checksumBytes, it throws and exception
        //channel.clearByteBuffer(checksumBytes);
//        checksumBytes.clear();
//        checksumBytes.limit(4);
//
//        if (channel.read(checksumBytes) != checksumBytes.limit())
//            throw new CorruptBlockException(getPath(), chunk);
//        return checksumBytes.getInt(0);
        return buffer.getInt(chunk.length + 1);
    }

    public int getTotalBufferSize()
    {
        return super.getTotalBufferSize() + compressed.capacity();
    }

    @Override
    public long length()
    {
        return metadata.dataLength;
    }

    @Override
    public String toString()
    {
        return String.format("%s - chunk length %d, data length %d.", getPath(), metadata.chunkLength(), metadata.dataLength);
    }

    public void deallocate()
    {
//        if (checksumBytes != null)
//            channel.destroyByteBuffer(checksumBytes);
        if (compressed != null)
            channel.destroyByteBuffer(compressed);
        super.deallocate();
    }
}
