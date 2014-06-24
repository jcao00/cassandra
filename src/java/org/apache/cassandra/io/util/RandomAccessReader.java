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

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.EOFException;
import java.io.File;
import java.nio.ByteBuffer;

public abstract class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // `bufferOffset` is the offset of the beginning of the buffer
    protected long bufferOffset;

    // `markedPointer` folds the offset of the last file mark
    protected long markedPointer;
    // absolute filesystem path to the file
    protected final String filePath;

    protected long fileLength;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;


    protected RandomAccessReader(File file, int bufferSize)
    {
        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");

        filePath = file.getAbsolutePath();
        buffer = allocateBuffer(bufferSize);
        buffer.limit(0);
    }

    protected abstract ByteBuffer allocateBuffer(int bufferSize);

    public abstract void deallocate();

    /* methods (re-)declared here to make them publicly visible */
    public abstract void close();

    public long getPosition()
    {
        return bufferOffset + buffer.position();
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

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
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

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    protected abstract void reBuffer();

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


    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long getFilePointer()
    {
        return current();
    }

    public String getPath()
    {
        return filePath;
    }

    public int getTotalBufferSize()
    {
        return buffer.capacity();
    }


    public long length()
    {
        return fileLength;
    }


    public long getPositionLimit()
    {
        return length();
    }

    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "')";
    }


    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (newPosition >= length()) // it is safe to call length() in read-only mode
        {
            if (newPosition > length())
                throw new IllegalArgumentException(String.format("unable to seek to position %d in %s (%d bytes) in read-only mode",
                        newPosition, getPath(), length()));
            buffer.limit(0);
            bufferOffset = newPosition;
            return;
        }

        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        bufferOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

}
