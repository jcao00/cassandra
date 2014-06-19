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
import java.nio.channels.FileChannel;

public class DirectReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(DirectReader.class);
    protected static final int ALIGNMENT = 512;// getAlignment();
    private int bufferMisalignment;

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

    protected final RateLimiter limiter;
    protected FileInputStream fis;

    protected DirectReader(File file, int bufferSize, RateLimiter limiter) throws IOException
    {
        super(file, bufferSize, null);
        this.limiter = limiter;
    }

    // called by ctor
    protected FileChannel openChannel(File file) throws IOException
    {
        int fd = CLibrary.tryOpenDirect(file.getAbsolutePath());
        if (fd < 0)
            throw new IOException("Unable to open file for direct i/o, return = {}" + fd);
        FileDescriptor fileDescriptor = CLibrary.setfd(fd);
        fis = new FileInputStream(fileDescriptor);
        return fis.getChannel();
    }

    // called by ctor
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
        return open(file, DEFAULT_BUFFER_SIZE, (RateLimiter)null);
    }

    public static RandomAccessReader open(File file, int bufferSize)
    {
        return open(file, bufferSize, (RateLimiter) null);
    }

    public static RandomAccessReader open(File file, int bufferSize, RateLimiter limiter)
    {
        try
        {
            //TODO: have some check if we're on linux and loaded the magick lib
            return new DirectReader(file, bufferSize, limiter);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void reBuffer()
    {
        reBuffer(buffer);
    }

    protected void reBuffer(ByteBuffer buf)
    {
        bufferOffset += buf.position();
        assert bufferOffset < fileLength;

        try
        {
            buf.clear();
            //adjust the buffer.limit if we have less bytes to read than current limit
            int readSize = buf.capacity();
            final long curPos = channel.position();
            if (curPos + buf.capacity() > fileLength)
                readSize = (int)(fileLength - curPos);
            if (limiter != null)
                limiter.acquire(readSize);

            //adjust for any misalignments in the channel offset - that is, start from an earlier
            // position in the channel (on the ALIGNMENT bound), then readjust the buffer's position later
            bufferMisalignment = (int)(bufferOffset % ALIGNMENT);
            channel.position(bufferOffset - bufferMisalignment);
            while (readSize > 0)
            {
                int n = channel.read(buf);
                if (n < 0)
                    break;
                readSize -= n;
            }
            buf.flip();
            buf.position(bufferMisalignment);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position() - bufferMisalignment);
    }

    public void close()
    {
        bufferOffset += buffer.position();

        if (buffer.isDirect())
            CLibrary.destroyBuffer(buffer);
        buffer = null;

        try
        {
            channel.close();
            FileUtils.closeQuietly(fis);
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }
}
