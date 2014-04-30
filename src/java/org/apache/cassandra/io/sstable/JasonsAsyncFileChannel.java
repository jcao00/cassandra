package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Future;

import org.hornetq.core.libaio.Native;

public class JasonsAsyncFileChannel extends AsynchronousFileChannel
{

    public JasonsAsyncFileChannel(Path path, Set<? extends OpenOption> options) throws IOException
    {
        int fd = Native.openFile(path.toFile().getAbsolutePath());
        if (-1 == fd)
        {
            throw new IOException("Error when opening file " + path.toFile());
        }
    }

    public long size() throws IOException
    {
        return 0;
    }

    public AsynchronousFileChannel truncate(long size) throws IOException
    {
        return null;
    }

    public void force(boolean metaData) throws IOException
    {

    }

    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler)
    {

    }

    public Future<FileLock> lock(long position, long size, boolean shared)
    {
        return null;
    }

    public FileLock tryLock(long position, long size, boolean shared) throws IOException
    {
        return null;
    }

    public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler)
    {

    }

    public Future<Integer> read(ByteBuffer dst, long position)
    {
        return null;
    }

    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler)
    {

    }

    public Future<Integer> write(ByteBuffer src, long position)
    {
        return null;
    }

    public boolean isOpen()
    {
        return false;
    }

    public void close() throws IOException
    {

    }
}
