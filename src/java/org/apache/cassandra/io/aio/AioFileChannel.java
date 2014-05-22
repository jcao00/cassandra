package org.apache.cassandra.io.aio;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileLock;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AioFileChannel extends AsynchronousFileChannel
{
    private static final Logger logger = LoggerFactory.getLogger(AioFileChannel.class);

    private final String fileName;
    private final int fd;
    private final Semaphore maxIOSemaphore;
    private static final AtomicLong idGen = new AtomicLong(0);
    private volatile AtomicBoolean opened = new AtomicBoolean(false);
    final Map<Long, CompletionWrapper<CountDownLatch>> submitted;

    //reference to master aio context
    private final ByteBuffer aioContext;

    public AioFileChannel(Path path, Set<? extends OpenOption> options, ByteBuffer aioContext) throws IOException
    {
        assert !options.contains(StandardOpenOption.WRITE) : "not supporting writing with async i/o";
        //TODO: give this a legit value
        int maxIO = 42;

        this.fileName = path.toFile().getAbsolutePath();
        fd = Native.open0(fileName);
        if (fd < 0)
            throw new AsyncFileException("Unable to open file " + fileName);

        maxIOSemaphore = new Semaphore(maxIO);
        this.aioContext = aioContext;
        submitted = new ConcurrentHashMap<>();
        opened.set(true);
    }

    public long size() throws IOException
    {
        checkOpened();
        long size = Native.size0(fd);
        if (size < 0)
            throw new AsyncFileException("could not read size of file " + fileName);
        return size;
    }

    public boolean isOpen()
    {
        return opened.get();
    }

    private void checkOpened()
    {
        if (!opened.get())
        {
            throw new RuntimeException("async file is not opened: " + fileName);
        }
    }

    public Future<Integer> read(ByteBuffer dst, long position)
    {
        throw new UnsupportedOperationException("not supporting this read() method yet");
    }

    public <A> void read(ByteBuffer dst, long filePosition, final A attachment, final CompletionHandler<Integer, ? super A> handler)
    {
        checkOpened();
        //fabricate some identifier....
        long id = idGen.incrementAndGet();

        //TODO: this is a problem as we're only gating on the submission of the event, not the actual IO activity
        maxIOSemaphore.acquireUninterruptibly();
        try
        {
            submitted.put(id, new CompletionWrapper<CountDownLatch>((CountDownLatch)attachment,
                    (CompletionHandler<Integer, CountDownLatch>)handler));
            int cnt = Native.read0(aioContext, this, id, dst, dst.remaining(), fd, filePosition);
            if (cnt != 1)
            {
                submitted.remove(id);
                throw new AsyncFileException("could not submit read request for file " + fileName + ", return = " + cnt);
            }
        }
        catch (Exception e)
        {
            submitted.remove(id);
            handler.failed(e, attachment);
        }
        finally
        {
            maxIOSemaphore.release();
        }
    }

    public void callback(long eventId, int status)
    {
        CompletionWrapper<CountDownLatch> callback = submitted.remove(eventId);
        if (callback == null)
        {
            logger.info("could not find id {} in the submission map", eventId);
            return;
        }

        if (status >= 0)
        {
            callback.handler.completed(status, callback.attachment);
        }
        else
        {
            callback.handler.failed(new AsyncFileException("failed!!!"), callback.attachment);
        }
    }

    public AsynchronousFileChannel truncate(long size) throws IOException
    {
        throw new UnsupportedOperationException("not supporting truncate yet in async i/o");
    }

    public void force(boolean metaData) throws IOException
    {
        throw new UnsupportedOperationException("not supporting writes yet in async i/o");
    }

    public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler)
    {
        throw new UnsupportedOperationException("not supporting locks yet in async i/o");
    }

    public Future<FileLock> lock(long position, long size, boolean shared)
    {
        throw new UnsupportedOperationException("not supporting locks yet in async i/o");
    }

    public FileLock tryLock(long position, long size, boolean shared) throws IOException
    {
        throw new UnsupportedOperationException("not supporting locks yet in async i/o");
    }

    public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler)
    {
        throw new UnsupportedOperationException("not supporting writes yet in async i/o");
    }

    public Future<Integer> write(ByteBuffer src, long position)
    {
        throw new UnsupportedOperationException("not supporting writes yet in async i/o");
    }

    public void close() throws IOException
    {
        if (!opened.compareAndSet(false, true))
            return;

        Native.close0(fd);

        AioFileChannelFactory.INSTANCE.close(new File(fileName).toPath());
    }
}
