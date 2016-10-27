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
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
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
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Pair;

public class StreamingInboundHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);

    private static final int BUFFER_SIZE = 1 << 14;  //  1 << 16 = 64k
    public static final int CHECKSUM_LENGTH = Integer.BYTES;

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 20; // 1 << 19 = 512Kb
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 22; // 1 << 22 = 4Mb

    enum State { HEADER_MAGIC, HEADER_LENGTH, HEADER_PAYLOAD, PAYLOAD, CLOSED }

    private final byte[] intByteBuffer = new byte[Integer.BYTES];

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
        pendingBuffers = new AppendingByteBufInputStream();
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

        pendingBuffers.append((ByteBuf) message);

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
        switch (state)
        {
            case HEADER_MAGIC:
                if (pendingBuffers.readableBytes() < 4)
                    return;
                pendingBuffers.read(intByteBuffer, 0, 4);
                MessagingService.validateMagic(Ints.fromByteArray(intByteBuffer));
                currentTransferContext = new FileTransferContext(ctx);
                state = State.HEADER_LENGTH;
                // fall-through
            case HEADER_LENGTH:
                if (pendingBuffers.readableBytes() < 4)
                    return;
                pendingBuffers.read(intByteBuffer, 0, 4);
                currentTransferContext.headerLength = Ints.fromByteArray(intByteBuffer);
                state = State.HEADER_PAYLOAD;
                // fall-through
            case HEADER_PAYLOAD:
                if (pendingBuffers.readableBytes() < currentTransferContext.headerLength)
                    return;
                handleHeader();
                state = State.PAYLOAD;
                // fall-through
            case PAYLOAD:
                handlePayload(ctx);
                break;
            case CLOSED:
                ctx.close();
        }
    }

    private void handleHeader() throws IOException
    {
        DataInputPlus in = new DataInputPlus.DataInputStreamPlus(pendingBuffers);
        FileMessageHeader header = FileMessageHeader.serializer.deserialize(in, protocolVersion);
        currentTransferContext.header = header;
        StreamSession session = StreamManager.instance.findSession(remoteAddress.getAddress(), new IncomingFileMessage(null, header));
        if (session == null)
            throw new IllegalStateException(String.format("unknown stream session: %s - %d", header.planId, header.sequenceNumber));

        currentTransferContext.session = session;
        if (currentTransferContext.header.isCompressed())
            currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(header.getCompressionInfo().chunks);
        else
            currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(header.sections);

        Pair<String, String> kscf = Schema.instance.getCF(header.cfId);
        ColumnFamilyStore cfs = null;
        if (kscf != null)
            cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        if (kscf == null || cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + header.cfId + " was dropped during streaming");
        }
        currentTransferContext.cfs = cfs;

        // only add to the transferQueue when we have the header-related stuffs ready
        transferQueue.add(currentTransferContext);
    }

    private void handlePayload(ChannelHandlerContext ctx) throws Exception
    {
        if (pendingBuffers.readableBytes() == 0)
            return;

        int drainedBytes;
        if (currentTransferContext.header.isCompressed())
            drainedBytes = drainCompressedSstableData(currentTransferContext, pendingBuffers, currentTransferContext.inputStream);
        else
            drainedBytes = drainUncompressedSstableData(pendingBuffers, currentTransferContext.inputStream);

        currentTransferContext.remaingPayloadBytesToReceive -= drainedBytes;

        // if we've reached the end of the current file transfer, and there's leftover bytes,
        // it means we've already started receiving the next file
        if (currentTransferContext.remaingPayloadBytesToReceive == 0)
        {
            state = State.HEADER_MAGIC;
            parseBufferedBytes(ctx);
        }
    }

    private static int drainUncompressedSstableData(AppendingByteBufInputStream src, AppendingByteArrayInputStream dst) throws IOException
    {
        int drainedBytes = 0;
        while (true)
        {
            int size = Math.min(BUFFER_SIZE, src.readableBytes());
            if (size == 0)
                break;
            byte[] buf = new byte[size];
            src.read(buf, 0 , size);
            dst.append(buf);
            drainedBytes += size;
        }
        return drainedBytes;
    }

    /**
     * Attempt to decompress as many chunks from the {@code src} as possible.
     */
    private int drainCompressedSstableData(FileTransferContext transferContext, AppendingByteBufInputStream src, AppendingByteArrayInputStream dst) throws IOException
    {
        CompressionInfo compressionInfo = transferContext.header.compressionInfo;
        // try to decompress as many blocks as possible
        int consumedBytes = 0;
        while (transferContext.currentCompressionChunk < compressionInfo.chunks.length)
        {
            CompressionMetadata.Chunk chunk = compressionInfo.chunks[transferContext.currentCompressionChunk];
            int readLength = chunk.length + CHECKSUM_LENGTH;
            if (src.readableBytes() < readLength)
                break;

            byte[] compressedBuffer = new byte[readLength];
            src.read(compressedBuffer, 0, readLength);
            dst.append(decompress(compressionInfo, compressedBuffer));
            transferContext.currentCompressionChunk++;
            consumedBytes += readLength;
        }
        return consumedBytes;
    }

    private byte[] decompress(CompressionInfo compressionInfo, byte[] src) throws IOException
    {
        byte[] dst = new byte[compressionInfo.parameters.chunkLength()];
        int compressedDataLen = src.length - CHECKSUM_LENGTH;
        compressionInfo.parameters.getSstableCompressor().uncompress(src, 0, compressedDataLen, dst, 0);

        // validate crc randomly
        Double crcCheckChance = currentTransferContext.cfs.getCrcCheckChance();
        if (crcCheckChance > ThreadLocalRandom.current().nextDouble())
        {
            ChecksumType checksumType = currentTransferContext.header.version.compressedChecksumType();
            int calculatedChecksum = (int) checksumType.of(src, 0, compressedDataLen);
            System.arraycopy(src, compressedDataLen, intByteBuffer, 0, CHECKSUM_LENGTH);

            if (calculatedChecksum != Ints.fromByteArray(intByteBuffer))
                throw new IOException("CRC unmatched");
        }

        return dst;
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
         * A queue for the incoming {@link ByteBuf}s, which will be processed by the {@link #blockingIOThread}. Items in this queue
         * live longer than {@link StreamingInboundHandler#pendingBuffers}, and hence we set the low/high water marks from this instance.
         */
        private final AppendingByteArrayInputStream inputStream;

        private int headerLength;
        private FileMessageHeader header;
        private StreamSession session;
        private long remaingPayloadBytesToReceive;
        private ColumnFamilyStore cfs;

        /**
         * If the target file is using sstable compression, this is the index into the header's {@link CompressionInfo#chunks}
         * that is currently being operated on.
         */
        private int currentCompressionChunk;

        private FileTransferContext(ChannelHandlerContext ctx)
        {
            inputStream = new AppendingByteArrayInputStream(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, ctx);
        }
    }

    /**
     * A task that can execute the blocking deserialization behavior of {@link StreamReader#read(ColumnFamilyStore, InputStream)} )} )}.
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
                    SSTableMultiWriter ssTableMultiWriter = reader.read(transferContext.cfs, transferContext.inputStream);
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
