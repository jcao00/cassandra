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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.SwappingByteBufDataOutputStreamPlus;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * StreamWriter writes given section of the SSTable to given channel.
 */
public class StreamWriter
{
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(StreamWriter.class);

    protected final SSTableReader sstable;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamRateLimiter limiter;
    protected final StreamSession session;

    private final StreamCompressionSerializer compressionSerializer;

    public StreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, StreamSession session)
    {
        this.session = session;
        this.sstable = sstable;
        this.sections = sections;
        this.limiter =  StreamManager.getRateLimiter(session.peer);
        compressionSerializer = new StreamCompressionSerializer(NettyFactory.lz4Factory().fastCompressor(),
                                                                NettyFactory.lz4Factory().fastDecompressor());
    }

    /**
     * Stream file of specified sections to given channel.
     *
     * StreamWriter uses LZF compression on wire to decrease size to transfer.
     *
     * @param output where this writes data to
     * @throws IOException on any I/O error
     */
    public void write(DataOutputStreamPlus output) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);

        Channel channel = ((SwappingByteBufDataOutputStreamPlus)output).channel();
        try(ChannelProxy proxy = sstable.getDataChannel().sharedCopy();
            ChecksumValidator validator = new File(sstable.descriptor.filenameFor(Component.CRC)).exists()
                                          ? DataIntegrityMetadata.checksumValidator(sstable.descriptor)
                                          : null)
        {
            int bufferSize = validator == null ? DEFAULT_CHUNK_SIZE: validator.chunkSize;

            // setting up data compression stream
            long progress = 0L;

            // stream each of the required sections of the file
            for (Pair<Long, Long> section : sections)
            {
                long start = validator == null ? section.left : validator.chunkStart(section.left);
                // if the transfer does not start on the valididator's chunk boundary, this is the number of bytes to offset by
                int transferOffset = (int) (section.left - start);
                if (validator != null)
                    validator.seek(start);

                // length of the section to read
                long length = section.right - start;
                // tracks write progress
                long bytesRead = 0;
                while (bytesRead < length)
                {
                    int toTransfer = (int) Math.min(bufferSize, length - bytesRead);
                    long lastBytesRead = write(proxy, validator, channel, start, transferOffset, toTransfer);
                    start += lastBytesRead;
                    bytesRead += lastBytesRead;
                    progress += (lastBytesRead - transferOffset);
                    session.progress(sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress, totalSize);
                    transferOffset = 0;
                }

                // make sure that current section is sent
                output.flush();
            }
            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                         session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize));
        }
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    /**
     * Sequentially read bytes from the file and write them to the output stream
     *
     * @param proxy The file reader to read from
     * @param validator validator to verify data integrity
     * @param start The readd offset from the beginning of the {@code proxy} file.
     * @param transferOffset number of bytes to skip transfer, but include for validation.
     * @param toTransfer The number of bytes to be transferred.
     *
     * @return Number of bytes transferred.
     *
     * @throws java.io.IOException on any I/O error
     */
    protected long write(ChannelProxy proxy, ChecksumValidator validator, Channel channel, long start, int transferOffset, int toTransfer) throws IOException
    {
        // the count of bytes to read off disk
        int minReadable = (int) Math.min(toTransfer, proxy.size() - start);

        ByteBuf outBuf = channel.alloc().directBuffer(minReadable, minReadable);
        ByteBuffer outNioBuffer = outBuf.nioBuffer(0, minReadable);
        long readCount = proxy.read(outNioBuffer, start);

        if (validator != null)
        {
            outNioBuffer.flip();
            validator.validate(outNioBuffer);
        }

        outBuf.writerIndex((int) readCount);
        outBuf.readerIndex(transferOffset);

        if (!waitUntilWritable(channel))
            throw new IOException("outbound channel was not writable");

        ByteBuf compressed = compressionSerializer.serialize(outBuf, StreamSession.CURRENT_VERSION);
        limiter.acquire(compressed.readableBytes());
        channel.writeAndFlush(compressed).addListener(future -> {
            if (!future.isSuccess() && channel.isOpen())
            {
                logger.error("sending file block failed", future.cause());
                channel.close();
                session.sessionFailed(); /// ????????
            }
        });

        return toTransfer;
    }

    protected boolean waitUntilWritable(Channel channel)
    {
        if (channel.isWritable())
            return true;

        // wait until the channel is writable again.
        SimpleCondition condition = new SimpleCondition();
        ChannelHandler writabilityHandler = new ChannelWritabiliityListsener(condition);
        channel.pipeline().addLast(writabilityHandler);
        try
        {
            condition.await(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            // TODO:JEB handle this better
            // nop
        }

        channel.pipeline().remove(writabilityHandler);
        if (!channel.isWritable())
        {
            logger.info("JEB:: waited on condition but channel is still unwritable");
            channel.close();
            session.sessionFailed(); /// ?????
            return false;
        }
        return true;
    }

    private static class ChannelWritabiliityListsener extends ChannelDuplexHandler
    {
        private final SimpleCondition condition;

        private ChannelWritabiliityListsener(SimpleCondition condition)
        {
            this.condition = condition;
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
        {
            if (ctx.channel().isWritable())
                condition.signalAll();
        }
    }
}
