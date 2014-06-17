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

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.io.FSReadError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Uses memory and block alignment, as well as DIO.
 *
 * Using native pread to get around using nio/nio2 (yeech!)
 */
public class DirectReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(DirectReader.class);

    static
    {
        try
        {
            System.loadLibrary("cassandra-dio");
        }
        catch (Throwable e)
        {
            logger.info("error loading the direct-io (dio) native library ");
        }
    }

    //alignment for memory pages
    protected static final int PAGE_ALIGNMENT = 512;
    //alignment for disk blocks
    protected static final int BLOCK_ALIGNMENT = 512;

    private final int fd;
    protected final RateLimiter limiter;
    protected final AtomicBoolean open = new AtomicBoolean(false);

    protected int bufferMisalignment;

    protected DirectReader(File file, int bufferSize, RateLimiter limiter) throws IOException
    {
        super(file, bufferSize);
        fd = tryOpenDirect(file.getAbsolutePath());
        if (fd < 0)
            throw new IOException(String.format("Unable to open %s for direct i/o, return = %d", filePath, fd));
        open.set(true);
        fileLength = getFileLength(fd);

        buffer = allocateBuffer(bufferSize);
        buffer.limit(0);
        this.limiter = limiter;
    }

    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        return allocateDirectBuffer(bufferSize);
    }

    protected ByteBuffer allocateDirectBuffer(int bufferSize)
    {
        // make the buffer memory-page aligned
        int size = (int) Math.min(fileLength, bufferSize);
        int alignOff = size % PAGE_ALIGNMENT;
        if (alignOff != 0)
            size += PAGE_ALIGNMENT - alignOff;

        return allocateNativeBuffer(size);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, null);
    }

    public static RandomAccessReader open(File file, RateLimiter limiter)
    {
        try
        {
            return new DirectReader(file, DEFAULT_BUFFER_SIZE, limiter);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void reBuffer()
    {
        fileOffset += buffer.position() - bufferMisalignment;
        assert fileOffset < fileLength;
        buffer.clear();
        buffer = reBuffer(buffer, fileOffset);
    }

    /**
     * @param dst the buffer to write into
     * @param readStart allow callers to state an explicit offset (allows CompressedDirectReader to override RAR.fileOffset)
     */
    protected ByteBuffer reBuffer(ByteBuffer dst, long readStart)
    {
        try
        {
            assert readStart < fileLength;

            int readSize = dst.limit() - dst.position();
            if (readStart + readSize > fileLength)
                readSize = (int)(fileLength - readStart);

            //adjust for any misalignments in the file offset - that is, start from an earlier
            // position in the file (on the BLOCK_ALIGNMENT bound), then readjust the buffer.position later.
            // in short, this aligns the read starting byte.
            bufferMisalignment = (int)(readStart % BLOCK_ALIGNMENT);
            readStart -= bufferMisalignment;
            readSize += bufferMisalignment;

            // this makes sure the number of bytes we want to read is disk-block aligned;
            // that is, adjust the readSize to be a multiple of BLOCK_ALIGNMENT
            int bufferMisalignmentFromEnd = readSize % BLOCK_ALIGNMENT;
            if (bufferMisalignmentFromEnd != 0)
            {
                bufferMisalignmentFromEnd = BLOCK_ALIGNMENT - bufferMisalignmentFromEnd;
                readSize += bufferMisalignmentFromEnd;
            }

            //resize the buffer to make sure it has enough capacity in case the the read size has been
            // enlarged due to any block alignment adjustments above
            if (dst.capacity() < readSize)
            {
                destroyNativeBuffer(dst);
                dst = allocateDirectBuffer(readSize);
            }
            else
            {
                dst.clear();
            }
            dst.limit(readSize);

            if (limiter != null)
                limiter.acquire(readSize);
            while (readSize > 0)
            {
                int cnt = tryPread(fd, dst, readSize, readStart);
                if (cnt == 0)
                    break;
                readStart += cnt;
                readSize -= cnt;
            }

            // now clean up alignment offset
            dst.flip();
            dst.position(bufferMisalignment);
            //only adjust this if we actually read the full readSize
            if (bufferMisalignmentFromEnd > 0 && readSize == 0)
                dst.limit(dst.limit() - bufferMisalignmentFromEnd);
            return dst;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    protected int bufferOffset()
    {
        return bufferMisalignment;
    }

    protected long current()
    {
        return fileOffset + (buffer == null ? 0 : buffer.position() - bufferMisalignment);
    }

    public long getPosition()
    {
        return fileOffset + buffer.position() - bufferMisalignment;
    }

    public void deallocate()
    {
        if (buffer != null)
        {
            fileOffset += buffer.position();

            if (buffer.isDirect())
                destroyNativeBuffer(buffer);
            buffer = null;
        }

        if (open.compareAndSet(true, false))
            tryCloseFD(fd);
    }

    public void close()
    {
        deallocate();
    }

    public int tryOpenDirect(String absolutePath) throws IOException
    {
        int fd = open0(absolutePath);
        if (fd < 0)
            throw new IOException("could not open file, status = " + fd);
        return fd;
    }

    private static native int open0(String file);

    public void tryCloseFD(int fd)
    {
        int status = close0(fd);
        if (status < 0)
            logger.info("unable to close fd {} for file {}", fd, filePath);
    }

    private static native int close0(int fd);

    public native ByteBuffer allocateNativeBuffer(long size);

    public native void destroyNativeBuffer(ByteBuffer buffer);

    public long getFileLength(int fd) throws IOException
    {
        long filesize = filesize0(fd);
        if (filesize < 0)
            throw new IOException("could not get filesize");
        return filesize;
    }

    private static native long filesize0(int fd);

    public int tryPread(int fd, ByteBuffer buf, int size, long offset) throws IOException
    {
        int status = pread0(fd, buf, size, offset);
        if (status < 0)
            throw new IOException(String.format("pread(%d) failed, errno (%d).", fd, status));
        buf.position(buf.position() + status);
        return status;
    }


    private static native int pread0(int fd, ByteBuffer buf, int size, long offset);
}
