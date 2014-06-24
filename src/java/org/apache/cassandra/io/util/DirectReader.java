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
import org.apache.cassandra.utils.CLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * Uses memory and block alignment, as well as DIO.
 *
 * Using native pread to get around using nio/nio2 (yeech!)
 */
public class DirectReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(DirectReader.class);
    protected static final int ALIGNMENT = 512;// getAlignment();

    private static int getAlignment()
    {
        try
        {
            Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (sun.misc.Unsafe) field.get(null);
            int pageSize = unsafe.pageSize();
            logger.info("alignment/pageSize = {}", pageSize);
            return pageSize;
        }
        catch (Exception e)
        {
            throw new RuntimeException("could not get page size alignment from Unsafe", e);
        }
    }

    private final int fd;
    protected final RateLimiter limiter;

    protected ByteBuffer buffer;
    private int bufferMisalignment;

    protected DirectReader(File file, int bufferSize, RateLimiter limiter) throws IOException
    {
        super(file, bufferSize);
        fd = CLibrary.tryOpenDirect(file.getAbsolutePath());
        if (fd < 0)
            throw new IOException("Unable to open file for direct i/o, return = {}" + fd);
        fileLength = CLibrary.getFileLength(fd);

        buffer = allocateBuffer(bufferSize);
        this.limiter = limiter;
    }

    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        int size = (int) Math.min(fileLength, bufferSize);
        int alignOff = size % ALIGNMENT;
        if (alignOff != 0)
            size += ALIGNMENT - alignOff;

        return CLibrary.allocateBuffer(size);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null);
    }

    public static RandomAccessReader open(File file, int bufferSize)
    {
        return open(file, bufferSize, null);
    }

    public static RandomAccessReader open(File file, int bufferSize, RateLimiter limiter)
    {
        try
        {
            return new DirectReader(file, bufferSize, limiter);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void reBuffer()
    {
        bufferOffset += buffer.position();
        assert bufferOffset < fileLength;
        buffer = reBuffer(buffer);
    }

    protected ByteBuffer reBuffer(ByteBuffer buf)
    {
        try
        {
            buf.clear();
            //adjust the buffer.limit if we have less bytes to read than current limit
            int readSize = buf.capacity();
            final long curPos = bufferOffset;
            if (curPos + buf.capacity() > fileLength)
                readSize = (int)(fileLength - curPos);

            //adjust for any misalignments in the file offset - that is, start from an earlier
            // position in the channel (on the ALIGNMENT bound), then readjust the buffer's position later
            bufferMisalignment = (int)(bufferOffset % ALIGNMENT);
            readSize += bufferMisalignment;
            if (buf.capacity() < readSize)
            {
                CLibrary.destroyBuffer(buf);
                buf = allocateBuffer(readSize);
            }
            long readStart = bufferOffset - bufferMisalignment;

            if (limiter != null)
                limiter.acquire(readSize);
            while (readSize > 0)
            {
                int n = CLibrary.tryPread(fd, buf, readSize, readStart);
                if (n < 0)
                    break;
                readSize -= n;
                readStart += n;
            }
            buf.flip();
            buf.position(bufferMisalignment);
            return buf;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position() - bufferMisalignment);
    }

    public void deallocate()
    {
        bufferOffset += buffer.position();

        if (buffer.isDirect())
            CLibrary.destroyBuffer(buffer);
        buffer = null;

        CLibrary.tryCloseFD(fd);
    }

    public void close()
    {
        deallocate();
    }
}
