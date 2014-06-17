package org.apache.cassandra.io.compress;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.DirectReader;
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

    protected CompressedDirectReader(File file, CompressionMetadata metadata, RateLimiter limiter) throws IOException
    {
        super(file, metadata.chunkLength(), limiter);
        this.metadata = metadata;
        checksum = metadata.hasPostCompressionAdlerChecksums ? new Adler32() : new CRC32();
        int len = metadata.compressor().initialCompressedBufferLength(metadata.chunkLength());
        compressed = allocateDirectBuffer(len < fileLength ? len : (int)fileLength);
    }

    // called be ctor, super.buffer does *not* need to be off-heap
    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        return ByteBuffer.allocate(bufferSize);
    }

    public static CompressedDirectReader open(File file, CompressionMetadata metadata)
    {
        return open(file, metadata, null);
    }

    public static CompressedDirectReader open(File file, CompressionMetadata metadata, RateLimiter limiter)
    {
        try
        {
            return new CompressedDirectReader(file, metadata, limiter);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void reBuffer()
    {
        try
        {
            final long position = current();
            assert position < metadata.dataLength;

            final CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
            final boolean readChecksum = metadata.parameters.getCrcCheckChance() > FBUtilities.threadLocalRandom().nextDouble();
            int readLen = chunk.length + (readChecksum ? 4 : 0);

            if (compressed.capacity() < readLen)
            {
                destroyNativeBuffer(compressed);
                compressed = allocateDirectBuffer(readLen);
            }
            else
            {
                compressed.clear();
            }
            compressed.limit(readLen);

            compressed = reBuffer(compressed, chunk.offset);
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

            if (readChecksum)
            {
                if (metadata.hasPostCompressionAdlerChecksums)
                {
                    checksum.update(onHeapCompressed, 0, chunk.length);
                }
                else
                {
                    checksum.update(buffer.array(), 0, decompressedBytes);
                }

                if (compressed.getInt(bufferMisalignment + chunk.length) != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            fileOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - fileOffset));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
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

    protected long current()
    {
        return fileOffset + (buffer == null ? 0 : buffer.position());
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

    public void deallocate()
    {
        if (compressed != null && compressed.isDirect())
            destroyNativeBuffer(compressed);
        compressed = null;
        super.deallocate();
    }
}
