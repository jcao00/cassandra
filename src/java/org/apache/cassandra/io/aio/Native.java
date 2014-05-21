package org.apache.cassandra.io.aio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Native
{
    private static final Logger logger = LoggerFactory.getLogger(Native.class);

    static
    {
        final String[] libraries = new String[]{ "cassandra-aio" };
        boolean loaded = false;
        for (String library : libraries)
        {
            if (loadLibrary(library))
            {
                loaded = true;
                break;
            }
        }
        if (!loaded)
        {
            logger.debug("Couldn't locate LibAIO Wrapper (sadpanda)");
        }
    }

    private static boolean loadLibrary(final String name)
    {
        try
        {
            System.loadLibrary(name);
            return true;
        }
        catch (Throwable e)
        {
            logger.debug("error loading the native library " + name);
            return false;
        }
    }

    /* epoll-specific calls */
    public static native int epollCreate();
    public static native int epollWait(int efd, int maxEvents);
    public static native int epollDestroy(int epollFd);

    public static native ByteBuffer createAioContext(int maxIo);
    public static native int pollAioEvents(ByteBuffer aioContext, int maxIo, int maxLoops);
    public static native int destroyAioContext(ByteBuffer aioContext);

    public static native long size0(int fd);

    /**
     * @return the number of items submitted for read; should be equal to 1, else something bad happened.
     */
    public static native int read0(ByteBuffer aioContext, AioFileChannel aioFileChannel, long id, ByteBuffer dst, int bufSize, int fd, long position);

    public static native int open0(String fileName);
    public static native int close0(int fd);

    public static native ByteBuffer newNativeBuffer(long size);
    public static native void resetBuffer(ByteBuffer directByteBuffer, int size);
    public static native void destroyBuffer(ByteBuffer buffer);
}
