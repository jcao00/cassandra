package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.apache.cassandra.io.util.BufferProvider.*;

public abstract class FileWrapper
{
    public enum IoStyle {normal, async, custom};

    private final BufferProvider bufferProvider;

    protected FileWrapper(BufferProvider bufferProvider)
    {

        this.bufferProvider = bufferProvider;
    }

    long size() throws IOException;

    void close() throws IOException;

    void position(long bufferOffset) throws IOException;

    long transferTo(long l, int toTransfer, WritableByteChannel channel) throws IOException;

    ByteBuffer allocateBuffer(int size)
    {
        return bufferProvider.allocateBuffer(size);
    }

    void resetByteBuffer(ByteBuffer buffer)
    {
        bufferProvider.resetByteBuffer(buffer);
    }

    void destroyByteBuffer(ByteBuffer buffer)
    {
        bufferProvider.destroyByteBuffer(buffer);
    }

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
                return new AsyncFileChannelWrapper(file, write, ioStyle);
            }
        }
    }

    static class FileChannelWrapper extends FileWrapper
    {
        private final FileChannel fileChannel;

        private FileChannelWrapper(File file, boolean write) throws IOException
        {
            super(NioBufferProvider.INSTANCE);
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

    static class AsyncFileChannelWrapper extends FileWrapper
    {
        private final AsynchronousFileChannel asyncFileChannel;
        private final ExecutorService executor;
        private volatile long offset;

        private AsyncFileChannelWrapper(File file, boolean write, IoStyle ioStyle) throws IOException
        {
            super(ioStyle == IoStyle.async ? NioBufferProvider.INSTANCE : NativeBufferProvider.INSTANCE);
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
            this.asyncFileChannel = AsynchronousFileChannel.open(file.toPath(), opts, executor, new FileAttribute[0]);
        }

        public long size() throws IOException
        {
            return asyncFileChannel.size();
        }

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            return ByteBuffer.allocateDirect(bufferSize);
        }

        public void resetByteBuffer(ByteBuffer buffer)
        {
            buffer.clear();
        }

        public void destroyByteBuffer(ByteBuffer buffer)
        {
            //nop
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
            try {
                LatchCompletionHandler handler = new LatchCompletionHandler();
                CountDownLatch cdl = new CountDownLatch(1);

                asyncFileChannel.read(buffer, offset, cdl, handler);
                cdl.await(2, TimeUnit.SECONDS);
                if (handler.ioe != null)
                    throw handler.ioe;

                offset += handler.cnt;
                return handler.cnt;
            }
            catch (InterruptedException e)
            {
                throw new IOException("error while reading file asynchronously", e);
            }
        }

        class LatchCompletionHandler implements CompletionHandler<Integer, CountDownLatch>
        {
            //either cnt ot exception will be populated, but not both
            int cnt;
            IOException ioe;

            public void completed(Integer result, CountDownLatch attachment)
            {
                cnt = result.intValue();
                attachment.countDown();
            }

            public void failed(Throwable exc, CountDownLatch attachment)
            {
                ioe = new IOException("filed to read block", exc);
                attachment.countDown();
            }
        }


        public long position()
        {
            //TODO: hope like hell this works ...
            return offset;
        }
    }
}
