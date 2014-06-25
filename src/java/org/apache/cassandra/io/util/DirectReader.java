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
import java.util.concurrent.atomic.AtomicBoolean;

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
    protected final AtomicBoolean open = new AtomicBoolean(false);

    protected int bufferMisalignment;

    protected DirectReader(File file, int bufferSize, RateLimiter limiter) throws IOException
    {
        super(file, bufferSize);
        fd = CLibrary.tryOpenDirect(file.getAbsolutePath());
        if (fd < 0)
            throw new IOException(String.format("Unable to open %s for direct i/o, return = %d", filePath, fd));
        open.set(true);
        fileLength = CLibrary.getFileLength(fd);

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
        bufferOffset += buffer.position() - bufferMisalignment;
        assert bufferOffset < fileLength;
        buffer.clear();
        buffer = reBuffer(buffer, bufferOffset);
    }

    protected ByteBuffer reBuffer(ByteBuffer dst, long readStart)
    {
        try
        {
            assert readStart < fileLength;

            int readSize = dst.limit() - dst.position();
            if (readStart + dst.capacity() > fileLength)
                readSize = (int)(fileLength - readStart);

            //adjust for any misalignments in the file offset - that is, start from an earlier
            // position in the channel (on the ALIGNMENT bound), then readjust the buffer's position later.
            // in short, this aligns the read starting byte.
            bufferMisalignment = (int)(readStart % ALIGNMENT);
            readStart -= bufferMisalignment;
            readSize += bufferMisalignment;

            // needed when we're at the end of file (or in a short file), and number of bytes to read is < ALIGNMENT,
            // tell pread we want an aligned read count, even though we'll we'll hit EOF before that.
            // in short, this makes sure the number of bytes we tell read() that we want to read is aligned
            int readSizeAlign = readSize;
            if (readSizeAlign % ALIGNMENT != 0)
            {
                readSizeAlign += ALIGNMENT - (readSizeAlign % ALIGNMENT);
            }

            if (dst.capacity() < readSizeAlign)
            {
                CLibrary.destroyBuffer(dst);
                dst = allocateDirectBuffer(readSizeAlign);
            }
            dst.limit(readSizeAlign);

            if (limiter != null)
                limiter.acquire(readSizeAlign);
            int cnt = 0;
            while (cnt < readSize)
            {
                int n = CLibrary.tryPread(fd, dst, readSizeAlign, readStart);
                if (n < 0)
                    break;
                cnt += n;
                dst.position(cnt);
                readStart += n;
            }

            // now clean up alignment offset
            dst.flip();
            dst.position(bufferMisalignment);
            return dst;
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
        if (buffer != null)
        {
            bufferOffset += buffer.position();

            if (buffer.isDirect())
                CLibrary.destroyBuffer(buffer);
            buffer = null;
        }

        if (open.compareAndSet(true, false))
        {
            logger.info("closing fd {}", fd);
            CLibrary.tryCloseFD(fd);
        }
    }

    public void close()
    {
        deallocate();
    }
}
