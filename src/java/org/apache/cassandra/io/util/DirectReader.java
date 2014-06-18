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
    private static final int ALIGNMENT = 512;// getAlignment();

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

    protected DirectReader(File file, int bufferSize, RateLimiter limiter) throws FileNotFoundException
    {
        super(file, bufferSize, null);
        this.limiter = limiter;
    }

    public static DirectReader open(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, (RateLimiter)null);
    }

    public static DirectReader open(File file, int bufferSize)
    {
        return open(file, bufferSize, (RateLimiter)null);
    }

    public static DirectReader open(File file, int bufferSize, RateLimiter limiter)
    {
        try
        {
            return new DirectReader(file, bufferSize, limiter);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected FileChannel openChannel(File file) throws IOException
    {
        int fd = CLibrary.tryOpenDirect(file.getAbsolutePath());
        if (fd < 0)
            throw new IOException("Unable to open file for direct i/o, return = {}" + fd);
        FileDescriptor fileDescriptor = CLibrary.setfd(fd);
        fis = new FileInputStream(fileDescriptor);
        return fis.getChannel();
    }

    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        int size = (int) Math.min(fileLength, bufferSize);
        int alignOff = size % ALIGNMENT;
        if (alignOff != 0)
            size += ALIGNMENT - alignOff;

        return ByteBuffer.allocateDirect(size);
    }

    protected void reBuffer()
    {
        bufferOffset += buffer.position();
        assert bufferOffset < fileLength;

        try
        {
            buffer.clear();
            //adjust the buffer.limit if we have less bytes to read than current limit
            long readSize = buffer.capacity();
            long curPos = channel.position();
            if (curPos + buffer.capacity() > fileLength - curPos)
                readSize = fileLength - curPos;
            if (limiter != null)
                limiter.acquire(buffer.limit());

            channel.position(bufferOffset); // setting channel position
            while (readSize > 0)
            {
                int n = channel.read(buffer);
                if (n < 0)
                    break;
                readSize -= n;
            }
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(fis);
        super.close();
    }
}
