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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    private static final Logger logger = LoggerFactory.getLogger(RandomAccessReader.class);
    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    protected final String filePath;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;

    // `fileOffset` is the offset of the beginning of the file
    // `markedPointer` folds the offset of the last file mark
    protected long fileOffset, markedPointer;

    protected long fileLength;

    protected RandomAccessReader(File file, int bufferSize) throws IOException
    {
        filePath = file.getAbsolutePath();

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     *
     * @return a new ByteBuffer instance if reallocation was necessary.
     */
    protected abstract void reBuffer();

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        return fileOffset + (buffer == null ? 0 : buffer.position());
    }

    public String getPath()
    {
        return filePath;
    }

    public int getTotalBufferSize()
    {
        return buffer.capacity();
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    public abstract void close();

    public abstract void deallocate();

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");
        if (newPosition > length())
            throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                    newPosition, getPath(), length()));

        if (newPosition == length())
        {
            buffer.limit(0);
            fileOffset = newPosition;
            return;
        }

        if (newPosition >= fileOffset && newPosition < fileOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - fileOffset) + bufferOffset());
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        fileOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

    protected int bufferOffset()
    {
        return 0;
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining())
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining())
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;
        try
        {
            ByteBuffer result = ByteBuffer.allocate(length);
            while (result.hasRemaining())
            {
                if (isEOF())
                    throw new EOFException();
                if (!buffer.hasRemaining())
                    reBuffer();
                ByteBufferUtil.put(buffer, result);
            }
            result.flip();
            return result;
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return fileOffset + buffer.position();
    }

    public long getPositionLimit()
    {
        return length();
    }
}
