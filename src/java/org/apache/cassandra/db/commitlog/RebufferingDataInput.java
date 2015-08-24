package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.AbstractDataInput;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;

/**
 * Similar in spirit to {@link org.apache.cassandra.io.util.ByteBufferDataInput}, but allows a backing buffer
 * (either from memory or disk) to be read in chunks, and transformed before being consumed.
 * This is useful if the backing buffer is compressed or encrypted data.
 */
public class RebufferingDataInput extends AbstractDataInput implements FileDataInput, DataInput
{
    private final String filename;
    private final long segmentOffset;
    private final int expectedLength;
    private final ChunkProvider chunkProvider;

    /**
     * current logical position through the input buffer(s).
     */
    private int position;

    /**
     * The current working buffer from which we serve up data to be processed by other consumers of this instance.
     */
    private ByteBuffer buffer;

    public RebufferingDataInput(String filename, long segmentOffset, int position, int expectedLength, ChunkProvider chunkProvider)
    {

        this.filename = filename;
        this.segmentOffset = segmentOffset;
        this.position = position;
        this.expectedLength = expectedLength;
        this.chunkProvider = chunkProvider;
    }

    public interface ChunkProvider
    {
        /**
         * Get the next chunk from the backing provider, if any chunks remain.
         * @return Next chunk, else null if no more chunks remain.
         */
        ByteBuffer nextChunk();
    }

    public String getPath()
    {
        return filename;
    }

    public long getFilePointer()
    {
        return segmentOffset + position;
    }

    public long getPosition()
    {
        return segmentOffset + position;
    }

    public long getPositionLimit()
    {
        return segmentOffset + expectedLength;
    }

    public boolean isEOF() throws IOException
    {
        return position >= expectedLength;
    }

    public long bytesRemaining() throws IOException
    {
        return expectedLength - position;
    }

    public void seek(long position) throws IOException
    {
        // implement this complexity when we actually need it
        throw new UnsupportedOperationException();
    }

    public FileMark mark()
    {
        // implement this complexity when we actually need it
        throw new UnsupportedOperationException();
    }

    public void reset(FileMark mark) throws IOException
    {
        // implement this complexity when we actually need it
        throw new UnsupportedOperationException();
    }

    public long bytesPastMark(FileMark mark)
    {
        // implement this complexity when we actually need it
        throw new UnsupportedOperationException();
    }

    public ByteBuffer readBytes(int length) throws IOException
    {
        ByteBuffer result = ByteBuffer.allocate(length);
        while (length > 0)
        {
            if (!maybeRebuffer())
                return ByteBuffer.allocate(0);

            ByteBuffer view = buffer.duplicate();
            int viewLength = view.remaining() > length ? length : view.remaining();
            view.limit(view.position() + viewLength);
            result.put(view);

            length -= viewLength;
            position += viewLength;
            buffer.position(buffer.position() + viewLength);
        }

        result.flip();
        return result;
    }

    /**
     * attempt to buffer more data, if necessary.
     *
     * @return false if no more data is available; else, true.
     */
    boolean maybeRebuffer()
    {
        if (buffer != null && buffer.remaining() > 0)
            return true;

        buffer = chunkProvider.nextChunk();

        if (buffer == null)
            return false;

        return true;
    }

    public int read() throws IOException
    {
        if (!maybeRebuffer())
            return -1;
        int b = buffer.get() & 0xFF;
        position++;
        return b;
    }
}
