package org.apache.cassandra.io.util;

import org.apache.cassandra.io.aio.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public interface BufferProvider
{
    static final Logger logger = LoggerFactory.getLogger(BufferProvider.class);

    ByteBuffer allocateBuffer(int size);
    void destroyByteBuffer(ByteBuffer buffer);

    public class NioBufferProvider implements BufferProvider
    {
        public static final NioBufferProvider INSTANCE = new NioBufferProvider();

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            return ByteBuffer.allocate(bufferSize);
        }

        public void destroyByteBuffer(ByteBuffer buffer)
        {
            //nop
        }
    }

    public class NativeBufferProvider implements BufferProvider
    {
        private static final int ALIGNMENT = 512;
        public static final NativeBufferProvider INSTANCE = new NativeBufferProvider();

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            final int offset = bufferSize % ALIGNMENT;
            if (offset != 0)
            {
                bufferSize += ALIGNMENT - offset;
            }
            logger.info("new buffer size = " + bufferSize);

            return Native.newNativeBuffer(bufferSize);
        }

        public void destroyByteBuffer(ByteBuffer buffer)
        {
            Native.destroyBuffer(buffer);
        }
    }
}
