package org.apache.cassandra.io.util;

import org.apache.cassandra.io.aio.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public interface BufferProvider
{
    static final Logger logger = LoggerFactory.getLogger(BufferProvider.class);

    ByteBuffer allocateBuffer(int size);
    void clearByteBuffer(ByteBuffer buffer);
    void destroyByteBuffer(ByteBuffer buffer);


    public class NioBufferProvider implements BufferProvider
    {
        public static final NioBufferProvider INSTANCE = new NioBufferProvider();

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            return ByteBuffer.allocate(bufferSize);
        }

        public void clearByteBuffer(ByteBuffer buffer)
        {
            buffer.clear();
        }

        public void destroyByteBuffer(ByteBuffer buffer)
        {
            //nop
        }
    }

    public class NativeBufferProvider implements BufferProvider
    {
        private static final int ALIGNMENT = 512;
        private int requestedSize;

        public synchronized ByteBuffer allocateBuffer(int bufferSize)
        {
            requestedSize = bufferSize;
            final int offset = bufferSize % ALIGNMENT;
            if (offset != 0)
            {
                bufferSize += ALIGNMENT - offset;
            }
            logger.info("new native buffer of size {}", bufferSize);
            ByteBuffer buf = Native.newNativeBuffer(bufferSize);
            if (buf == null)
                throw new RuntimeException("failed to create a native buffer of size " + bufferSize);
            buf.limit(requestedSize);
            return buf;
        }

        public void clearByteBuffer(ByteBuffer buffer)
        {
            buffer.clear();
            // need to set the limit as BB.clear() will set limit = capacity. however, capacity
            //has been set to a memaligned size, and will almost always be larger than the intended size
            buffer.limit(requestedSize);
        }

        public synchronized void destroyByteBuffer(ByteBuffer buffer)
        {
            Native.destroyBuffer(buffer);
        }
    }
}
