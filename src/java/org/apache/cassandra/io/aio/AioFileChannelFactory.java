package org.apache.cassandra.io.aio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maintains a map of opened files, as well as a shared epoll context.
 */
public class AioFileChannelFactory
{
    private static final Logger logger = LoggerFactory.getLogger(AioFileChannelFactory.class);
    public static final AioFileChannelFactory INSTANCE = new AioFileChannelFactory();

    private final Map<Path, AioFileChannel> openFiles;

//    @VisibleForTesting
    ByteBuffer aioContext;
    private final ExecutorService executorService;
    private final int maxEvents;
    private volatile boolean open;
    private final Lock openLock = new ReentrantLock();

    private volatile boolean runPoller;
    private final Lock pollerLock = new ReentrantLock();

    private AioFileChannelFactory()
    {
        //TODO: give this a legit value
        this.maxEvents = 1024;
        aioContext = Native.createAioContext(maxEvents);
        if (aioContext == null)
        {
            throw new RuntimeException("Could not open aio context");
        }

        openFiles = new ConcurrentHashMap<>();
        executorService = Executors.newSingleThreadExecutor(new NamedThreadFactory("AsyncFilePoller"));
        open = true;
    }

    public AioFileChannel newAioFileChannel(Path path, Set<? extends OpenOption> options) throws IOException
    {
        if (!open)
            throw new IllegalStateException("Factory is closed due to file system being shut down");

        AioFileChannel afc = openFiles.get(path);
        if (afc != null)
            return afc;

        openLock.lock();
        try
        {
            afc = openFiles.get(path);
            if (afc != null)
                return afc;
            afc = new AioFileChannel(path, options, aioContext);
            openFiles.put(path, afc);

            if (!runPoller)
            {
                pollerLock.lock();
                try
                {
                    executorService.execute(new PollRunnable());
                    runPoller = true;
                }
                finally
                {
                    pollerLock.unlock();
                }
            }
            return afc;
        }
        finally
        {
            openLock.unlock();
        }
    }

    // callback form AioFileChannels when they are closed
    public void close(Path path)
    {
        if (openFiles.remove(path) == null)
        {
            logger.debug("file already removed (or not found) from open file cache: {}", path);
        }

        if (openFiles.size() == 0)
        {
            pollerLock.lock();
            try
            {
                if (openFiles.size() == 0)
                {
                    runPoller = false;
                }
            }
            finally
            {
                pollerLock.unlock();
            }
        }
    }

    public void close()
    {
        if (!open)
            return;
        open = false;
        executorService.shutdown();

        for (Map.Entry<Path, AioFileChannel> entry : openFiles.entrySet())
        {
            try
            {
                entry.getValue().close();
                close(entry.getKey());
            }
            catch (IOException e)
            {
                logger.info("could not close file {}", entry.getKey());
            }
        }

        int ret = Native.destroyAioContext(aioContext);
        if (ret < 0)
            logger.warn("problem while closing the aio context. ignoring, but error code " + ret);

        //TODO: check if we can simply null the BB here, or if any other magic needs to happen
        aioContext = null;
    }

    class NamedThreadFactory implements ThreadFactory
    {
        protected final String id;
        protected final AtomicInteger n = new AtomicInteger(1);

        public NamedThreadFactory(String id)
        {
            this.id = id;
        }

        public Thread newThread(Runnable runnable) {
            String name = id + ":" + n.getAndIncrement();
            Thread thread = new Thread(runnable, name);
            thread.setPriority(Thread.NORM_PRIORITY);
            thread.setDaemon(true);
            return thread;
        }
    }

    private class PollRunnable implements Runnable
    {
        private static final int MAX_LOOPS = 8;

        public void run()
        {
            while (runPoller)
            {
                Native.pollAioEvents(aioContext, maxEvents, MAX_LOOPS);
            }
        }
    }
}
