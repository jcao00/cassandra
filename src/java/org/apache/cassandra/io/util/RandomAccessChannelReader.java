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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.FSReadError;

public class RandomAccessChannelReader extends RandomAccessReader
{
    // channel linked with the file, used to retrieve data and force updates.
    protected final FileChannel channel;

    protected final PoolingSegmentedFile owner;

    protected RandomAccessChannelReader(File file, int bufferSize, PoolingSegmentedFile owner) throws IOException
    {
        super(file, bufferSize);
        this.owner = owner;
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);

        // we can cache file length in read-only mode
        try
        {
            fileLength = channel.size();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }

        buffer = allocateBuffer(bufferSize);
        buffer.limit(0);
    }

    protected ByteBuffer allocateBuffer(int bufferSize)
    {
        return ByteBuffer.allocate((int) Math.min(fileLength, bufferSize));
    }

    // only called from BuffPoolingSegFile
    public static RandomAccessReader open(File file, PoolingSegmentedFile owner)
    {
        return open(file, DEFAULT_BUFFER_SIZE, owner);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, DEFAULT_BUFFER_SIZE, null);
    }

    @VisibleForTesting
    static RandomAccessReader open(File file, int bufferSize, PoolingSegmentedFile owner)
    {
        try
        {
            return new RandomAccessChannelReader(file, bufferSize, owner);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static RandomAccessReader open(SequentialWriter writer)
    {
        return open(new File(writer.getPath()), DEFAULT_BUFFER_SIZE, null);
    }

    // channel extends FileChannel, impl SeekableByteChannel.  Safe to cast.
    public FileChannel getChannel()
    {
        return channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        fileOffset += buffer.position();
        buffer.clear();
        assert fileOffset < fileLength;

        try
        {
            channel.position(fileOffset); // setting channel position
            while (buffer.hasRemaining())
            {
                int n = channel.read(buffer);
                if (n < 0)
                    break;
            }
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public void close()
    {
        if (owner == null || buffer == null)
        {
            // The buffer == null check is so that if the pool owner has deallocated us, calling close()
            // will re-call deallocate rather than recycling a deallocated object.
            // I'd be more comfortable if deallocate didn't have to handle being idempotent like that,
            // but RandomAccessFile.close will call AbstractInterruptibleChannel.close which will
            // re-call RAF.close -- in this case, [C]RAR.close since we are overriding that.
            deallocate();
        }
        else
        {
            owner.recycle(this);
        }
    }

    public void deallocate()
    {
        fileOffset += buffer.position();
        buffer = null; // makes sure we don't use this after it's ostensibly closed

        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }
}
