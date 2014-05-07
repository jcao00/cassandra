package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

public interface FileWrapper
{
    public enum IoStyle {normal, async};

    long size() throws IOException;

    void close() throws IOException;

    void position(long bufferOffset) throws IOException;

    long transferTo(long l, int toTransfer, WritableByteChannel channel) throws IOException;

    int read(ByteBuffer buffer) throws IOException;

    long position() throws IOException;

    public static class Factory
    {
        public static FileWrapper get(IoStyle ioStyle, File file, boolean write) throws IOException
        {
            if (ioStyle == IoStyle.normal)
            {
                return new FileChannelWrapper(file, write);
            }
            else
            {
                return new AsyncFileChannelWrapper(file, write);
            }
        }
    }

    static class FileChannelWrapper implements FileWrapper
    {
        private final FileChannel fileChannel;

        private FileChannelWrapper(File file, boolean write) throws IOException
        {
            if (write)
            {
                this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            }
            else
            {
                this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
            }
        }

        public long size() throws IOException
        {
            return fileChannel.size();
        }

        public void close() throws IOException
        {
            fileChannel.close();
        }

        public void position(long bufferOffset) throws IOException
        {
            if (fileChannel.position() != bufferOffset)
                fileChannel.position(bufferOffset);
        }

        public long transferTo(long l, int toTransfer, WritableByteChannel channel) throws IOException
        {
            return fileChannel.transferTo(l, toTransfer, channel);
        }

        public int read(ByteBuffer buffer) throws IOException
        {
            return fileChannel.read(buffer);
        }

        public long position() throws IOException
        {
            return fileChannel.position();
        }
    }

    static class AsyncFileChannelWrapper implements FileWrapper
    {
        private final AsynchronousFileChannel asyncFileChannel;
        private final ExecutorService executor;
        private volatile long offset;

        private AsyncFileChannelWrapper(File file, boolean write) throws IOException
        {
            executor = new ForkJoinPool();
            Set<StandardOpenOption> opts = new HashSet<>();
            if (write)
            {
                opts.add(StandardOpenOption.CREATE_NEW);
                opts.add(StandardOpenOption.WRITE);
            }
            else
            {
                opts.add(StandardOpenOption.READ);
            }
            this.asyncFileChannel = AsynchronousFileChannel.open(file.toPath(), opts, executor, null);
        }

        public long size() throws IOException
        {
            return asyncFileChannel.size();
        }

        public void close() throws IOException
        {
            //best effort to stop all outstanding requests
            executor.shutdownNow();
            asyncFileChannel.close();
        }

        public void position(long bufferOffset)
        {
            offset = bufferOffset;
        }

        public long transferTo(long l, int toTransfer, WritableByteChannel channel) throws IOException
        {
            //TODO: actually impl this!
            return 0;
        }

        public int read(ByteBuffer buffer) throws IOException
        {
            try
            {
                int cnt = asyncFileChannel.read(buffer, offset).get();
                offset += cnt;
                return cnt;
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new IOException("error while reading file asynchronously", e);
            }
        }

        public long position()
        {
            //TODO: hope like hell this works ...
            return offset;
        }
    }
}
