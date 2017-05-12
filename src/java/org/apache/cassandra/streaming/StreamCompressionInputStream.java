/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class StreamCompressionInputStream extends InputStream
{
    private final StreamCompressionSerializer compressionSerializer;

    private final ReadableByteChannel channel;
    private final int protocolVersion;
    private final byte[] oneByteArray = new byte[1];

    private ByteBuffer currentDecompressedBuffer;

    public StreamCompressionInputStream(ReadableByteChannel channel, int protocolVersion, StreamCompressionSerializer compressionSerializer)
    {
        this.channel = channel;
        this.protocolVersion = protocolVersion;
        this.compressionSerializer = compressionSerializer;
    }

    @Override
    public int read() throws IOException
    {
        int retVal = read(oneByteArray, 0 ,1);
        return retVal == -1 ? -1 : oneByteArray[0] & 0xFF;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException
    {
        int remaining = len;
        while (remaining > 0)
        {
            if (currentDecompressedBuffer == null || !currentDecompressedBuffer.hasRemaining())
                currentDecompressedBuffer = compressionSerializer.deserialize(channel, protocolVersion);

            int readSize = Math.min(remaining, currentDecompressedBuffer.remaining());
            ByteBufferUtil.arrayCopy(currentDecompressedBuffer, currentDecompressedBuffer.position(), b, off, readSize);
            currentDecompressedBuffer.position(currentDecompressedBuffer.position() + readSize);
            off += readSize;
            remaining -= readSize;
        }

        return len;
    }
}
