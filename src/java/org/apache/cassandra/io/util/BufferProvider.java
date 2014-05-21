package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public interface BufferProvider
{
    ByteBuffer allocateBuffer(int size);
    void resetByteBuffer(ByteBuffer buffer);
    void destroyByteBuffer(ByteBuffer buffer);

    public class NioBufferProvider implements BufferProvider
    {
        public static final NioBufferProvider INSTANCE = new NioBufferProvider();

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            return ByteBuffer.allocate(bufferSize);
        }

        public void resetByteBuffer(ByteBuffer buffer)
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
        public static final NativeBufferProvider INSTANCE = new NativeBufferProvider();

        public ByteBuffer allocateBuffer(int bufferSize)
        {
            return ByteBuffer.allocate(bufferSize);
        }

        public void resetByteBuffer(ByteBuffer buffer)
        {
            buffer.clear();
        }

        public void destroyByteBuffer(ByteBuffer buffer)
        {
            //nop
        }
    }
}
