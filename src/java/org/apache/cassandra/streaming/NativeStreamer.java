package org.apache.cassandra.streaming;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketImpl;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

public class NativeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(NativeStreamer.class);

    static
    {
        final String[] libraries = new String[]{ "cassandra-stream" };
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
            logger.warn("Couldn't locate streaming native lib (sadpanda)");
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
            logger.info("error loading the native library " + name, e);
            return false;
        }
    }

    // this implementation inspired by http://dev.xscheme.de/2013/05/getting-the-file-descriptor-integers-from-a-socket-java/
    private static Field fdField;
    private static Method socketMethod;
    private static Method socketImplMethod;
    static
    {
        try
        {
            fdField = FileDescriptor.class.getDeclaredField("fd");
            fdField.setAccessible(true);

            socketMethod = Socket.class.getDeclaredMethod("getImpl");
            socketMethod.setAccessible(true);

            socketImplMethod = SocketImpl.class.getDeclaredMethod("getFileDescriptor");
            socketImplMethod.setAccessible(true);
        }
        catch (Exception e)
        {
            throw new RuntimeException("cannot modify fd field of FileDescriptor class");
        }
    }

    public static void write(RandomAccessReader reader, long offset, int length, WritableByteChannel writableByteChannel) throws IOException
    {
        assert writableByteChannel instanceof SocketChannel;
        try
        {
            // if you like reflection, you'll love the four, back-to-back invoke()s to uncover
            // the file descriptors from the fabulous java nio api
            int fd = fdField.getInt(reader.getFD());
            Socket socket = ((SocketChannel)writableByteChannel).socket();
            SocketImpl socketImpl = (SocketImpl)socketMethod.invoke(socket);
            FileDescriptor fileDescriptor = (FileDescriptor)socketImplMethod.invoke(socketImpl);
            int outFd = fdField.getInt(fileDescriptor);

            // if you got this far without error, congratulate yourself with a trip over the jni-barrier.
            // do not pass go, do not collect 200 dollars, go straight to rage-ville.
            int status = write0(fd, offset, length, outFd);
        }
        catch (Exception e)
        {
            throw new RuntimeException("cannot read fds", e);
        }
    }

    private static native int write0(int fd, long offset, int len, int outFd);
}
