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

package org.apache.cassandra.streaming.async;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.AppendingByteBufInputStream;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingUtils;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;

public class StreamingInboundHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);

    public static final int CRC_LENGTH = Integer.BYTES;

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 20; // 1 << 19 = 500Kb
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 30; // 1 << 22 = 4Mb

    enum State { HEADER_MAGIC, HEADER_LENGTH, HEADER_PAYLOAD, PAYLOAD, CLOSED }

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    private State state;
    private FileTransferContext currentTransferContext;

    /**
     * A collection of buffers that cannot be consumed yet as more data needs to arrive. This is applicable to waiting
     * for a sett set of bytes to deserialize the headers, or for waiting for an entire chunk of compressed data to arrive
     * (before it can be decompressed).
     */
    private AppendingByteBufInputStream pendingBuffers;

    /**
     * A queue of {@link FileTransferContext}s that is used for correctly delineating the bounds of incoming files
     * for the {@link #blockingIOThread}.
     */
    private final BlockingQueue<FileTransferContext> transferQueue;

    /**
     * A background thread that performs the deserialization of the sstable data.
     */
    private Thread blockingIOThread;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        transferQueue = new LinkedBlockingQueue<>();
        state = State.HEADER_MAGIC;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        pendingBuffers = new AppendingByteBufInputStream(ctx);
        blockingIOThread = new FastThreadLocalThread(new DeserializingSstableTask(ctx, remoteAddress, transferQueue),
                                                     String.format("Stream-Inbound-%s", remoteAddress.toString()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception
    {
        if (!(message instanceof ByteBuf))
        {
            ctx.fireChannelRead(message);
            return;
        }

        // perform checks on the buffer up front to avoid doing them later (and possibly duplicating the work)
        ByteBuf buf = (ByteBuf) message;
        if (!buf.isReadable())
        {
            buf.release();
            return;
        }

        pendingBuffers.append(buf);

        try
        {
            parseBufferedBytes(ctx);
        }
        catch (Exception e)
        {
            logger.error("error while deserializing file message and headers", e);
            state = State.CLOSED;
            // TODO:JEB inform bg-thread
            ctx.close();
        }
    }

    private void parseBufferedBytes(ChannelHandlerContext ctx) throws Exception
    {
        DataInputPlus in = null;
        switch (state)
        {
            case HEADER_MAGIC:
                if (pendingBuffers.readableBytes() < 4)
                    return;
                in = new DataInputPlus.DataInputStreamPlus(pendingBuffers);
                int magic = in.readInt();
                MessagingService.validateMagic(magic);
                currentTransferContext = new FileTransferContext(ctx);
                state = State.HEADER_LENGTH;
                // fall-through
            case HEADER_LENGTH:
                if (pendingBuffers.readableBytes() < 4)
                    return;
                if (in == null)
                    in = new DataInputPlus.DataInputStreamPlus(pendingBuffers);
                currentTransferContext.headerLength = in.readInt();
                state = State.HEADER_PAYLOAD;
                // fall-through
            case HEADER_PAYLOAD:
                if (pendingBuffers.readableBytes() < currentTransferContext.headerLength)
                    return;
                if (in == null)
                    in = new DataInputPlus.DataInputStreamPlus(pendingBuffers);
                currentTransferContext.header = FileMessageHeader.serializer.deserialize(in, protocolVersion);
                StreamSession session = StreamManager.instance.findSession(remoteAddress.getAddress(), new IncomingFileMessage(null, currentTransferContext.header));
                if (session == null)
                    throw new IllegalStateException(String.format("unknown stream session: %s - %d", currentTransferContext.header.planId, currentTransferContext.header.sequenceNumber));

                currentTransferContext.session = session;
                if (currentTransferContext.header.isCompressed())
                    currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(currentTransferContext.header.getCompressionInfo().chunks);
                else
                    currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(currentTransferContext.header.sections);

                // only add to the transferQueue when we have the header-related stuffs ready
                transferQueue.add(currentTransferContext);
                state = State.PAYLOAD;
                // fall-through
            case PAYLOAD:
                handlePayload(ctx);
                break;
            case CLOSED:
                ctx.close();
        }
    }

    private void handlePayload(ChannelHandlerContext ctx) throws Exception
    {
        if (pendingBuffers.readableBytes() == 0)
            return;

        int drainedBytes;
        if (!currentTransferContext.header.isCompressed())
            drainedBytes = pendingBuffers.drain(currentTransferContext.remaingPayloadBytesToReceive, byteBuf -> currentTransferContext.inputStream.append(byteBuf));
        else
            drainedBytes = drain(ctx, currentTransferContext, pendingBuffers, byteBuf -> currentTransferContext.inputStream.append(byteBuf));

        currentTransferContext.remaingPayloadBytesToReceive -= drainedBytes;

        // if we've reached the end of the current file transfer, and there's leftover bytes,
        // it means we've already started receiving the next file
        if (currentTransferContext.remaingPayloadBytesToReceive == 0)
        {
            state = State.HEADER_MAGIC;
            parseBufferedBytes(ctx);
        }
    }

    /**
     * Attempt to decompress as many chunks from the {@code pendingBuffers} as possible.
     */
    private static int drain(ChannelHandlerContext ctx, FileTransferContext transferContext, AppendingByteBufInputStream pendingBuffers, Consumer<ByteBuf> consumer) throws IOException
    {
        CompressionInfo compressionInfo = transferContext.header.compressionInfo;
        // try to decompress as many blocks as possible
        int consumedBytes = 0;
        while (transferContext.currentCompressionChunk < compressionInfo.chunks.length)
        {
            CompressionMetadata.Chunk chunk = compressionInfo.chunks[transferContext.currentCompressionChunk];
            int readLength = chunk.length + CRC_LENGTH;
            if (pendingBuffers.readableBytes() < readLength)
                break;

//            logger.info("**** next chunk size = {}", readLength);
            // we can combine all queued buffers into one becuase we now have enough bytes to satisfy the next chunk size.
            // further, the bytes need to be in one buffer to satifsy the ICompressor API.
            List<ByteBuf> bufs = new ArrayList<>(8);
            int drainCount = pendingBuffers.drain(readLength, bufs::add);
//            logger.info("**** bufs count = {}, drainCOunt = {}", bufs.size(), drainCount);


            ByteBuf compressedChunk;
            if (bufs.size() == 1)
            {
                compressedChunk = bufs.get(0);
            }
            else
            {
                ByteBuf aggregatedBuf = new CompositeByteBuf(ctx.alloc(), true, bufs.size(), bufs);
                // note: cumulate is responsible for calling release() on the bufs, so we do not need to manage the buffer
                compressedChunk = ctx.alloc().buffer(readLength, readLength);
                compressedChunk = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), compressedChunk, aggregatedBuf);
            }

            ByteBuf decompressedChunk = decompress(ctx, compressionInfo, compressedChunk, chunk.length);
            consumer.accept(decompressedChunk);
            transferContext.currentCompressionChunk++;
            consumedBytes += readLength;
        }
        return consumedBytes;
    }

    private static ByteBuf decompress(ChannelHandlerContext ctx, CompressionInfo compressionInfo, ByteBuf compressedChunk, int chunkCompressedLength) throws IOException
    {
        ByteBuffer srcBuffer = compressedChunk.nioBuffer(compressedChunk.readerIndex(), chunkCompressedLength);
        final int chunkUncompressedLength = compressionInfo.parameters.chunkLength();
        ByteBuf decompressedChunk = ctx.alloc().buffer(chunkUncompressedLength, chunkUncompressedLength);
        ByteBuffer destBuffer = decompressedChunk.nioBuffer(0, chunkUncompressedLength);

        compressionInfo.parameters.getSstableCompressor().uncompress(srcBuffer, destBuffer);
        decompressedChunk.writerIndex(destBuffer.position());
        compressedChunk.readerIndex(compressedChunk.readerIndex() + chunkCompressedLength); // not sure this one is necessary

        // TODO:JEB put this shit back in
        // validate crc randomly
//            if (this.crcCheckChanceSupplier.get() > ThreadLocalRandom.current().nextDouble())
//            {
//                int checksum = (int) checksumType.of(compressed, 0, compressed.length - checksumBytes.length);
//
//                System.arraycopy(compressed, compressed.length - checksumBytes.length, checksumBytes, 0, checksumBytes.length);
//                if (Ints.fromByteArray(checksumBytes) != checksum)
//                    throw new IOException("CRC unmatched");
//            }

        return decompressedChunk;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    void close()
    {
        if (state == State.CLOSED)
            return;

        state = State.CLOSED;
        // TODO:JEB release resources;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing streaming file", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

    private static class FileTransferContext
    {
        /**
         * A queue for the incoming {@link ByteBuf}s, which will be processed by the {@link #blockingIOThread}.
         */
        private final AppendingByteBufInputStream inputStream;

        private int headerLength;
        private FileMessageHeader header;
        private StreamSession session;
        private long remaingPayloadBytesToReceive;

        /**
         * If the target file is using sstable compression, this is the index into the header's {@link CompressionInfo#chunks}
         * that is currently being operated on.
         */
        private int currentCompressionChunk;

        private FileTransferContext(ChannelHandlerContext ctx)
        {
            inputStream = new AppendingByteBufInputStream(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx);
        }
    }

    /**
     * A task that can execute the blocking deserialization behavior of {@link StreamReader#read(java.io.InputStream)}.
     */
    private class DeserializingSstableTask implements Runnable
    {
        private final ChannelHandlerContext ctx;
        private final InetSocketAddress remoteAddress;
        private final BlockingQueue<FileTransferContext> queue;

        DeserializingSstableTask(ChannelHandlerContext ctx, InetSocketAddress remoteAddress, BlockingQueue<FileTransferContext> queue)
        {
            this.ctx = ctx;
            this.remoteAddress = remoteAddress;
            this.queue = queue;
        }

        @Override
        public void run()
        {
            FileMessageHeader header = null;
            try
            {
                while (state != State.CLOSED)
                {
                    FileTransferContext transferContext = queue.poll(1, TimeUnit.SECONDS);
                    if (transferContext == null)
                        continue;

                    header = transferContext.header;

                    StreamReader reader = new StreamReader(header, transferContext.session);
                    SSTableMultiWriter ssTableMultiWriter = reader.read(transferContext.inputStream);
                    transferContext.session.receive(new IncomingFileMessage(ssTableMultiWriter, header));
                }
            }
            catch (EOFException e)
            {
                // thrown when netty socket closes/is interrupted
                logger.debug("eof reading from socket; closing");
            }
            catch (Throwable t)
            {
                // Throwable can be Runtime error containing IOException.
                // In that case we don't want to retry.
                // TODO:JEB resolve this
//                Throwable cause = t;
//                while ((cause = cause.getCause()) != null)
//                {
//                    if (cause instanceof IOException)
//                        throw (IOException) cause;
//                }
//                JVMStabilityInspector.inspectThrowable(t);
                logger.error("failed in streambackground thread", t);
            }
            finally
            {

                // TODO:JEB do we close the session or send complete somewheres?

                //StreamingInboundHandler.this.state = true;
                //FileUtils.closeQuietly(inputStream);
            }
        }
    }
}
