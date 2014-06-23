package org.apache.cassandra.io.compress;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.DirectReader;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class CompressedDirectReader extends DirectReader
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedDirectReader.class);

    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Checksum checksum;

    // raw checksum bytes
    private final ByteBuffer checksumBytes = ByteBuffer.wrap(new byte[4]);

    private int rebufferInvokeCnt;

    protected CompressedDirectReader(File file, int bufferSize, CompressionMetadata metadata, RateLimiter limiter) throws IOException
    {
        super(file, metadata.chunkLength(), limiter);
        this.metadata = metadata;
        checksum = metadata.hasPostCompressionAdlerChecksums ? new Adler32() : new CRC32();
        compressed = super.allocateBuffer(metadata.chunkLength() < fileLength ? metadata.chunkLength() : (int)fileLength);
    }

    // called be ctor, that buffer does *not* need to be off-heap
    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        assert Integer.bitCount(bufferSize) == 1;
        return ByteBuffer.allocate(bufferSize);
    }

    public static CompressedDirectReader open(File file, int bufferSize, CompressionMetadata metadata)
    {
        return open(file, bufferSize, metadata, null);
    }

    public static CompressedDirectReader open(File file, int bufferSize, CompressionMetadata metadata, RateLimiter limiter)
    {
        try
        {
            return new CompressedDirectReader(file, bufferSize, metadata, limiter);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void reBuffer()
    {
        rebufferInvokeCnt++;
        try
        {
            final long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            if (channel.position() != chunk.offset)
                channel.position(chunk.offset);

            final boolean mustReadChecksum = shouldReadChecksumBytes();
            int readLen = chunk.length + (mustReadChecksum ? 4 : 0);

            if (compressed.capacity() < readLen)
            {
                CLibrary.destroyBuffer(compressed);
                compressed = super.allocateBuffer(readLen);
            }

            reBuffer(compressed);
            // make sure we read the number of bytes we actually care about (first byte of buffer will be at the block alignment, so account for the offset)
            if (compressed.limit() - compressed.position() < readLen)
                throw new CorruptBlockException(getPath(), chunk);

            buffer.clear();
            final int decompressedBytes;
            final byte[] onHeapCompressed;
            try
            {
                // need this on heap as DirectBB doesn't support array()
                // interestingly, nio impl (FileChannel.read()) does more or less the same thing - create a tmp direct buffer
                // for the data from the file, then copy into an on-heap buffer for app use.
                onHeapCompressed = getBytes(compressed, chunk.length);
                decompressedBytes = metadata.compressor().uncompress(onHeapCompressed, 0, chunk.length, buffer.array(), 0);
                buffer.limit(decompressedBytes);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }

            if (mustReadChecksum)
            {
                if (metadata.hasPostCompressionAdlerChecksums)
                {
                    checksum.update(onHeapCompressed, 0, chunk.length);
                }
                else
                {
                    checksum.update(buffer.array(), 0, decompressedBytes);
                }

                if (compressed.getInt(chunk.length) != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
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

    boolean shouldReadChecksumBytes()
    {
        return metadata.parameters.getCrcCheckChance() > FBUtilities.threadLocalRandom().nextDouble();
    }

    private byte[] getBytes(ByteBuffer buffer, int length)
    {
        // need to copy on-heap if the buffer is a DirectBB ... <sigh>
        if (buffer.isDirect())
        {
            byte[] b = new byte[length];
            buffer.get(b);
            return b;
        }
        return buffer.array();
    }

    public int getTotalBufferSize()
    {
        return super.getTotalBufferSize() + compressed.capacity();
    }

    public long length()
    {
        return metadata.dataLength;
    }

    public String toString()
    {
        return String.format("%s - chunk length %d, data length %d.", getPath(), metadata.chunkLength(), metadata.dataLength);
    }

    public void close()
    {
        CLibrary.destroyBuffer(compressed);
        compressed = null;
        super.close();
    }

}
