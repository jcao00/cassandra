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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.DefaultFileRegion;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Used when tranferring whole or subsections of an sstable, and thus only the DATA component is shipped.
 * Responsible for determining the bounds of the next chunk of the sstable to transfer.
 *
 * With zero-copy transfers, the file needs to be handled in chunks, primarily for rate limiting:
 * for example, if we try to acquire 100GB from the limiter, we could starve other requestors or be starved ourself.
 * Thus, we transfer in {@link #ZERO_COPY_CHUNK_SIZE} chunks.
 *
 * All of the derived classes implement the "modes" behavior as defined in the package-level documentation of
 * {@link org.apache.cassandra.streaming.async}.
 *
 * This class is not thread-safe.
 */
abstract class SstableChunker implements Iterator<Object>
{
    /**
     * Chunk size used on the non-zero-copy path. Indicates maximum file data to transfer on each chunk, and that data
     * will be pulled into user-space memory for various manipulations (see class-level documentation).
     */
    static final int NON_ZERO_COPY_CHUNK_SIZE = 64 * 1024;

    /**
     * Chunk size used on the zero-copy path. Indicates maximum file data to transfer on each chunk, but none
     * is pulled directly into user-space memory.
     */
    static final int ZERO_COPY_CHUNK_SIZE = 10 * 1024 * 1024;

    /**
     * It is assumed that the entities in this list are: non-overlapping, in-order, and within the bounds
     * of the target file. The first two are not critical, but the last one is.
     */
    private final List<Pair<Long, Long>> sections;

    final int chunkSize;
    final ChannelProxy channelProxy;
    private int sectionIdx;

    long nextStartPosition;
    long nextEndPosition;

    private boolean isEOF;
    private boolean needsRecalucaltion = true;

    SstableChunker(OutgoingFileMessage ofm, int chunkSize)
    {
        channelProxy = ofm.ref.get().getDataChannel();
        this.chunkSize = chunkSize;
        this.sections = ofm.header.sections;
    }

    SstableChunker(OutgoingFileMessage ofm, CompressionInfo compressionInfo, int chunkSize)
    {
        channelProxy = ofm.ref.get().getDataChannel();
        this.chunkSize = chunkSize;
        sections = getTransferSections(compressionInfo.chunks);
    }

    // chunks are assumed to be sorted by offset
    private static List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks)
    {
        List<Pair<Long, Long>> transferSections = new ArrayList<>();
        Pair<Long, Long> lastSection = null;
        for (CompressionMetadata.Chunk chunk : chunks)
        {
            if (lastSection != null)
            {
                if (chunk.offset == lastSection.right)
                {
                    // extend previous section to end of this chunk
                    lastSection = Pair.create(lastSection.left, chunk.offset + chunk.length + 4); // 4 bytes for CRC
                }
                else
                {
                    transferSections.add(lastSection);
                    lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
                }
            }
            else
            {
                lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
            }
        }
        if (lastSection != null)
            transferSections.add(lastSection);
        return transferSections;
    }

    public Iterator<Object> iterator()
    {
        return this;
    }

    public boolean hasNext()
    {
        if (needsRecalucaltion)
            calculateNextPositions();

        return !isEOF;
    }

    /**
     * Determine the values for {@link #nextStartPosition} and {@link #nextEndPosition}, taking into account where in the
     * current {@link #sections} we are as well the {@link #chunkSize}.
     */
    @VisibleForTesting
    void calculateNextPositions()
    {
        // check if we're at the end
        if (isEOF || sections.isEmpty() || (sectionIdx == sections.size() - 1 && nextEndPosition == sections.get(sectionIdx).right))
        {
            isEOF = true;
            needsRecalucaltion = false;
            return;
        }

        Pair<Long, Long> currentSection = sections.get(sectionIdx);
        nextStartPosition = nextEndPosition;

        // we're at the very beginning
        if (nextStartPosition <= 0)
        {
            assert sectionIdx == 0;
            nextStartPosition = currentSection.left;
        }
        // we're at the end of the current section
        else if (nextEndPosition >= currentSection.right)
        {
            sectionIdx++;
            if (sectionIdx >= sections.size())
            {
                isEOF = true;
                return;
            }
            currentSection = sections.get(sectionIdx);
            nextStartPosition = currentSection.left;
        }
        // we're partially into the current section
        else
        {
            nextStartPosition = nextEndPosition;
        }

        final long length = currentSection.right - nextStartPosition;
        final int toTransfer = (int) Math.min(chunkSize, length);
        nextEndPosition = nextStartPosition + toTransfer;
        nudgeCaclulatedPositions(currentSection);
        needsRecalucaltion = false;
    }

    /**
     * Extension point for subclasses to nudge the offsets.
     */
    void nudgeCaclulatedPositions(Pair<Long, Long> currentSection)
    {
        // nop
    }

    /**
     * {@inheritDoc}
     *
     * Gets the next transferable chunk of the ssatble. The returned object depends on cases as defined in the
     * package-level documentation, but, in brief, if we're doing a zero-copy transfer, a {@link DefaultFileRegion}
     * is returned, else a {@link ByteBuf} is returned.
     */
    @Override
    public Object next()
    {
        Object retVal = getNext();
        needsRecalucaltion = true;
        return retVal;
    }

    protected abstract Object getNext();

    /**
     * Performs a simple read of the file from the {@link #channelProxy}.
     */
    @VisibleForTesting
    static ByteBuf readChunk(ByteBufAllocator allocator, ChannelProxy proxy, long startPosition, long endPosition)
    {
        // this will be a reasonable buffer size due to the calculations in calculateNextPositions()
        int size = (int)(endPosition - startPosition);
        ByteBuf buf = allocator.buffer(size, size);
        ByteBuffer buffer = buf.nioBuffer(0, size);
        proxy.read(buffer, startPosition);

        buf.writerIndex(buffer.position());

        return buf;
    }
}

/**
 * Chunker for an uncompressed sstable.
 */
class UncompressedSstableChunker extends SstableChunker
{
    private final ByteBufAllocator allocator;
    private final ChecksumValidator validator;

    private long nextValidatorStartPosition;
    private long nextValidatorEndPosition;

    @VisibleForTesting
    UncompressedSstableChunker(ByteBufAllocator allocator, OutgoingFileMessage ofm, ChecksumValidator validator, int chunkSize)
    {
        super(ofm, chunkSize);
        this.allocator = allocator;
        this.validator = validator;
    }

    static UncompressedSstableChunker create(ByteBufAllocator allocator, OutgoingFileMessage ofm) throws IOException
    {
        if (new File(ofm.ref.get().descriptor.filenameFor(Component.CRC)).exists())
        {
            ChecksumValidator validator = DataIntegrityMetadata.checksumValidator(ofm.ref.get().descriptor);
            return new UncompressedSstableChunker(allocator, ofm, validator, validator.chunkSize);
        }
        return new UncompressedSstableChunker(allocator, ofm, null, NON_ZERO_COPY_CHUNK_SIZE);
    }

    @Override
    void nudgeCaclulatedPositions(Pair<Long, Long> currentSection)
    {
        if (validator != null)
        {
            // nudge the nextEndPosition down to align on the checksum chuck's boundary. we may end up with a small
            // block to stream on the first pass through here, but it simplifies the complexity of this code and will be dramitaclly more efficient
            // wrt disk use (not rereading data if not aligned on the checksum boundary) and cpu use (not recalculating the checksum for the same block).
            nextValidatorStartPosition = validator.chunkStart(nextStartPosition);
            nextValidatorEndPosition = nextValidatorStartPosition + chunkSize;

            if (currentSection.right > nextValidatorEndPosition)
                nextEndPosition = nextValidatorEndPosition;
            else if (nextValidatorEndPosition > channelProxy.size()) // we may be at the end of the file
                nextValidatorEndPosition = channelProxy.size();
        }
    }

    @Override
    protected Object getNext()
    {
        if (validator != null)
            return readChunk(channelProxy, validator, nextStartPosition, nextEndPosition);
        else
            return readChunk(allocator, channelProxy, nextStartPosition, nextEndPosition);
    }

    /**
     * Performs a read of the file from the {@link #channelProxy}, and then validates the checksum. The complicating factor
     * for this method is that checksum validation may need to read extra bytes versus what we'll actually transfer
     * due to the bounds of the checksummed block. All the logic for calculating positions for the bytes
     * to stream versus the bytes to read are all handled in {@link #calculateNextPositions()}.
     */
    @VisibleForTesting
    private ByteBuf readChunk(ChannelProxy proxy, ChecksumValidator validator,
                              long streamStartPosition, long streamEndPosition)
    {
        final int readSize = (int)(nextValidatorEndPosition - nextValidatorStartPosition);

        // read
        ByteBuffer byteBuffer = ByteBuffer.allocate(readSize);
        proxy.read(byteBuffer, nextValidatorStartPosition);
        byteBuffer.flip();

        // validate
        try
        {
            validator.seek(nextValidatorStartPosition);
            validator.validate(byteBuffer);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // adjust the buf's indices
        ByteBuf buf = Unpooled.wrappedBuffer(byteBuffer.array());
        final int endOffset = (int)(nextValidatorEndPosition - streamEndPosition);
        buf.writerIndex(byteBuffer.limit() - endOffset);
        buf.readerIndex((int)(streamStartPosition - nextValidatorStartPosition));

        return buf;
    }
}

/**
 * Chunker for a file using sstable compression that can be streamed using zero-copy transfer.
 */
class CompressedSstableChunker extends SstableChunker
{
    CompressedSstableChunker(OutgoingFileMessage ofm, CompressionInfo compressionInfo)
    {
        super(ofm, compressionInfo, ZERO_COPY_CHUNK_SIZE);
    }

    /**
     * In order to perform zero-copy transfers with netty, a {@link DefaultFileRegion} instance *must* be used as
     * the netty transport, and it's C bindings, are hard coded to *only* do zero-copy operations with a {@link DefaultFileRegion}.
     * However, {@link DefaultFileRegion} requires a reference to a {@link FileChannel}, which we *do not* hand out
     * from {@link SSTableReader}; instead those file channel(s) are internalized behind a {@link ChannelProxy} which doesn't expose
     * the {@link FileChannel}. All this to say, we need to create our own {@link FileChannel}, and fortunately netty
     * appears to do the correct handling of that resource wrt cleanup ({@link DefaultFileRegion#deallocate()}).
     * We should be safe wrt the {@link Ref} protecting the sstable because it is acquired before the {@link OutgoingFileMessage}
     * is created, and released when the stream session completes (successfully or not). #fingerscrossed
     */
    @Override
    protected Object getNext()
    {
        try
        {
            @SuppressWarnings("resources") // the stream & channel will be closed by DefaultFileRegion#deallocate
            FileInputStream fileInputStream = new FileInputStream(channelProxy.filePath());
            return new DefaultFileRegion(fileInputStream.getChannel(), nextStartPosition, nextEndPosition - nextStartPosition);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}

/**
 * Chunker for a file using sstable compression that is to be sent using TLS encryption.
 */
class SecureCompressedSstableChunker extends SstableChunker
{
    private final ByteBufAllocator allocator;

    SecureCompressedSstableChunker(OutgoingFileMessage ofm, CompressionInfo compressionInfo, ByteBufAllocator allocator)
    {
        super(ofm, compressionInfo, NON_ZERO_COPY_CHUNK_SIZE);
        this.allocator = allocator;
    }

    @Override
    protected Object getNext()
    {
        return readChunk(allocator, channelProxy, nextStartPosition, nextEndPosition);
    }
}