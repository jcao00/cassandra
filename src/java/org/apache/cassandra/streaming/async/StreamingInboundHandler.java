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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ChecksumMismatchException;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.AppendingByteBufInputPlus;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingUtils;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.streaming.messages.StreamMessage.Type;
import org.apache.cassandra.utils.ChecksumType;

import static org.apache.cassandra.streaming.async.StreamingOutboundHandler.MESSAGE_PREFIX_LENGTH;

/**
 * Handles the inbound side of streaming sstable data. From the incoming data, we reify partitions and rows
 * and write those out to new sstable files. Because deserialization is a blocking affair, we can't block
 * the netty event loop. Thus we have a background thread perform all the blocking deserialization.
 *
 * When a buffer arrives at {@link #channelRead(ChannelHandlerContext, Object)}, it is enqueued in {@link #pendingBuffers}.
 * Those buffers are then preprecessed (see next paragraph), and then passed along to {@link FileTransferContext#inputStream}
 * where it is consumed on the background thread by deserializing the data.
 *
 * If the incoming sstable is using sstable compression (see package-level documentation), we will decompress
 * each chunk, on the event loop, before enqueuing the decompressed buffer into {@link FileTransferContext#inputStream}.
 * If the incoming sstable is using stream compression, the stream of the data file, after the meassage headers,
 * will be compressed on the fly by the source. Those buffers will be decompressed on the event loop before being
 * enqueued into {@link FileTransferContext#inputStream}.
 *
 *  * // TODO:JEB document
 * - netty low/high water marks & how it relates to our use of auto-read
 * - netty auto-read, and why we do it on the event loop (keeps things simple from the netty dispatch POV)
 */
public class StreamingInboundHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingInboundHandler.class);

    public static final int CHECKSUM_LENGTH = Integer.BYTES;

    private static final int AUTO_READ_LOW_WATER_MARK = 1 << 19; // 1 << 19 = 512Kb
    private static final int AUTO_READ_HIGH_WATER_MARK = 1 << 21; // 1 << 22 = 4Mb

    enum State { START, MESSAGE_PAYLOAD, FILE_TRANSFER_HEADER_PAYLOAD, FILE_TRANSFER_PAYLOAD, CLOSED }

    private final byte[] intByteBuffer = new byte[Integer.BYTES];

    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    private State state;
    private FileTransferContext currentTransferContext;

    /**
     * A queue of {@link FileTransferContext}s that is used for correctly delineating the bounds of incoming files
     * for the {@link #blockingIOThread}.
     */
    private final BlockingQueue<FileTransferContext> transferQueue;

    /**
     * A collection of {@link ByteBuf}s that are yet to be processed. Incoming buffers are first dropped into this
     * structure, and then consumed.
     */
    private AppendingByteBufInputPlus pendingBuffers;

    /**
     * A background thread that performs the deserialization of the sstable data.
     */
    private Thread blockingIOThread;

    private StreamCompressionSerializer deserializer;

    public StreamingInboundHandler(InetSocketAddress remoteAddress, int protocolVersion)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
        transferQueue = new LinkedBlockingQueue<>();
        state = State.START;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        pendingBuffers = new AppendingByteBufInputPlus(ctx.channel().config());
        blockingIOThread = new FastThreadLocalThread(new DeserializingSstableTask(ctx, remoteAddress, transferQueue),
                                                     String.format("Stream-Inbound-%s", remoteAddress.toString()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception
    {
        if (state == State.CLOSED)
        {
            ReferenceCountUtil.release(message);
            return;
        }

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

    @VisibleForTesting
    void parseBufferedBytes(ChannelHandlerContext ctx) throws IOException
    {
        ChecksumType checksum = ChecksumType.CRC32;
        while (true)
        {
            switch (state)
            {
                case START:
                    if (pendingBuffers.available() < MESSAGE_PREFIX_LENGTH)
                        return;
                    MessagingService.validateMagic(pendingBuffers.readInt());
                    Type type = Type.getById(pendingBuffers.readByte());
                    currentTransferContext = new FileTransferContext(ctx.channel().config(), type);
                    currentTransferContext.headerLength = pendingBuffers.readInt();
                    int headerLengthChecksum = pendingBuffers.readInt();
                    int derivedLengthChecksum = (int) checksum.of(ByteBuffer.allocate(4).putInt(0, currentTransferContext.headerLength));
                    if (headerLengthChecksum != derivedLengthChecksum)
                        throw new ChecksumMismatchException("checksum mismatch on header length checksum");

                    state = type.isTransfer() ? State.FILE_TRANSFER_HEADER_PAYLOAD : State.MESSAGE_PAYLOAD;
                    continue;
                case MESSAGE_PAYLOAD:
                    if (pendingBuffers.available() < currentTransferContext.headerLength)
                        return;
                    handleControlMessage(ctx);
                    state = State.START;
                    continue;
                case FILE_TRANSFER_HEADER_PAYLOAD:
                    if (pendingBuffers.available() < currentTransferContext.headerLength)
                        return;
                    handleHeader(checksum);
                    currentTransferContext.session.attach(ctx.channel());
                    state = State.FILE_TRANSFER_PAYLOAD;
                    continue;
                case FILE_TRANSFER_PAYLOAD:
                    if (handlePayload(ctx) == 0)
                        return;
                    if (currentTransferContext.remaingPayloadBytesToReceive == 0)
                        state = State.START;
                    continue;
                default:
                    throw new IllegalStateException("unhandled state: " + state);
            }
        }
    }

    /**
     * Deserialize a control {@link StreamMessage} and process it.
     */
    private void handleControlMessage(ChannelHandlerContext ctx) throws IOException
    {
        StreamMessage message = currentTransferContext.type.getSerializer().deserialize(pendingBuffers, protocolVersion);

        if (message instanceof StreamInitMessage)
        {
            StreamInitMessage init = (StreamInitMessage)message;
            StreamResultFuture.initReceivingSide(init.sessionIndex, init.planId, init.description, init.from, init.from,
                                                 ctx.channel(), init.keepSSTableLevel, init.isIncremental, init.pendingRepair);
        }

        StreamSession session = StreamManager.instance.findSession(remoteAddress.getAddress(), message.planId, message.sessionIndex);
        // this may attempt to re-add a channel that's already been added to the session, but it's safe operation
        // and we don't send many control messages
        session.attach(ctx.channel());
        session.receive(message);
    }

    /**
     * Deserializes the header.
     */
    @VisibleForTesting
    void handleHeader(ChecksumType checksum) throws IOException
    {
        // in order to run the checksum, we need to copy the bytes into one buffer as we can't know if the
        // header bytes are in one ByteBuf (from which we could get an nioBuffer)
        byte[] b = new byte[currentTransferContext.headerLength];
        pendingBuffers.readFully(b);
        int sentChecksum = pendingBuffers.readInt();
        int derivedChecksum = (int)checksum.of(b, 0, b.length);
        if (sentChecksum != derivedChecksum)
            throw new ChecksumMismatchException();

        FileMessageHeader header = FileMessageHeader.serializer.deserialize(new ByteBufDataInputPlus(Unpooled.wrappedBuffer(b)), protocolVersion);
        StreamSession session = StreamManager.instance.findSession(remoteAddress.getAddress(), header.planId, header.sessionIndex);
        if (session == null)
            throw new IllegalStateException(String.format("unknown stream session: %s - %d", header.planId, header.sequenceNumber));
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(header.tableId);
        if (cfs == null)
            throw new IOException("CF " + header.tableId + " was dropped during streaming");

        currentTransferContext.header = header;
        currentTransferContext.session = session;
        currentTransferContext.cfs = cfs;

        if (header.isCompressed())
            currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(header.getCompressionInfo().chunks);
        else
            currentTransferContext.remaingPayloadBytesToReceive = StreamingUtils.totalSize(header.sections);

        transferQueue.add(currentTransferContext);
    }

    private int handlePayload(ChannelHandlerContext ctx) throws IOException
    {
        if (pendingBuffers.available() == 0)
            return 0;

        final int consumedBytes;
        if (currentTransferContext.header.isCompressed())
            consumedBytes = drainCompressedSstableData(currentTransferContext, pendingBuffers, currentTransferContext.inputStream);
        else
            consumedBytes = drainStreamCompressedData(pendingBuffers, currentTransferContext.inputStream);

        currentTransferContext.remaingPayloadBytesToReceive -= consumedBytes;
        return consumedBytes;
    }

    /**
     * Attempt to decompress as many chunks from the {@code src} as possible.
     */
    private int drainCompressedSstableData(FileTransferContext transferContext, AppendingByteBufInputPlus src, AppendingByteBufInputPlus dst) throws IOException
    {
        CompressionInfo compressionInfo = transferContext.header.compressionInfo;
        // try to decompress as many blocks as possible
        int consumedBytes = 0;
        while (transferContext.currentCompressionChunk < compressionInfo.chunks.length)
        {
            CompressionMetadata.Chunk chunk = compressionInfo.chunks[transferContext.currentCompressionChunk];
            int readLength = chunk.length + CHECKSUM_LENGTH;
            if (src.available() < readLength)
                break;

            byte[] compressedBuffer = new byte[readLength];
            src.readFully(compressedBuffer, 0, readLength);
            ByteBuf decompressed = Unpooled.wrappedBuffer(decompress(compressionInfo, compressedBuffer));
            decompressed.writerIndex(compressionInfo.parameters.chunkLength());
            dst.append(decompressed);
            transferContext.currentCompressionChunk++;
            consumedBytes += readLength;
        }
        return consumedBytes;
    }

    /**
     * Decompress, and possibly validate, the next chunk of the data stream for a compressed file. It's also possible
     * that the chunk is not compressed: if the size of {@code src} is equal to ({@link CompressionParams#maxCompressedLength()},
     * we assume the chunk is uncompressed.
     */
    private byte[] decompress(CompressionInfo compressionInfo, byte[] src) throws IOException
    {
        final int dataLength = src.length - CHECKSUM_LENGTH;
        final byte[] dst;
        final int compressedDataLen;
        // uncompress
        if (dataLength < compressionInfo.parameters.maxCompressedLength())
        {
            dst = new byte[compressionInfo.parameters.chunkLength()];
            ICompressor compressor = compressionInfo.parameters.getSstableCompressor();
            compressedDataLen = compressor.uncompress(src, 0, dataLength, dst, 0);
        }
        else
        {
            dst = src;
            compressedDataLen = src.length - CHECKSUM_LENGTH;
        }

        // validate crc randomly
        final Double crcCheckChance = currentTransferContext.cfs.getCrcCheckChance();
        if (crcCheckChance > 0d && crcCheckChance > ThreadLocalRandom.current().nextDouble())
        {
            // as of 4.0, checksum type is CRC32
            int calculatedChecksum = (int) ChecksumType.CRC32.of(src, 0, compressedDataLen);
            System.arraycopy(src, compressedDataLen, intByteBuffer, 0, CHECKSUM_LENGTH);

            if (calculatedChecksum != Ints.fromByteArray(intByteBuffer))
                throw new IOException("CRC unmatched");
        }

        return dst;
    }

    private int drainStreamCompressedData(AppendingByteBufInputPlus src, AppendingByteBufInputPlus dst) throws IOException
    {
        long remainingToReceive = currentTransferContext.remaingPayloadBytesToReceive;
        int drainedBytes = 0;

        while (remainingToReceive > 0)
        {
            if (deserializer == null)
                deserializer = new StreamCompressionSerializer(NettyFactory.lz4Factory().fastCompressor(),
                                                               NettyFactory.lz4Factory().fastDecompressor());
            ByteBuf buf = deserializer.deserialize(src, protocolVersion);
            if (buf == null)
                break;

            remainingToReceive -= buf.readableBytes();
            drainedBytes += buf.readableBytes();
            dst.append(buf);
        }
        return drainedBytes;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    private void close()
    {
        if (state == State.CLOSED)
            return;

        state = State.CLOSED;
        blockingIOThread.interrupt();
        // TODO:JEB release resources;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing streaming file", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

    /**
     * For testing
     */
    State getState()
    {
        return state;
    }

    /**
     * For testing only!!
     */
    void setPendingBuffers(AppendingByteBufInputPlus pendingBuffers)
    {
        this.pendingBuffers = pendingBuffers;
    }

    /**
     * For testing only
     */
    FileTransferContext getCurrentTransferContext()
    {
        return currentTransferContext;
    }

    /**
     * Struct to maintain the state of the current file being streamed in.
     */
    static class FileTransferContext
    {
        /**
         * A queue for the incoming {@link ByteBuf}s, which will be processed by the {@link #blockingIOThread}.
         *
         * // TODO:JEB confirm this next comment
         * Items in this queue
         * live longer than {@link StreamingInboundHandler#pendingBuffers}, and hence we set the low/high water marks from this instance.
         */
        final AppendingByteBufInputPlus inputStream;
        private final Type type;

        int headerLength;
        FileMessageHeader header;
        StreamSession session;
        long remaingPayloadBytesToReceive;
        ColumnFamilyStore cfs;

        /**
         * If the target file is using sstable compression, this is the index into the header's {@link CompressionInfo#chunks}
         * that is currently being operated on.
         */
        private int currentCompressionChunk;

        private FileTransferContext(ChannelConfig config, Type type)
        {
            this.type = type;
            if (type.isTransfer())
                inputStream = new AppendingByteBufInputPlus(AUTO_READ_LOW_WATER_MARK, AUTO_READ_HIGH_WATER_MARK, config);
            else
                inputStream = null;
        }
    }

    /**
     * A task that can execute the blocking deserialization behavior of {@link StreamReader#read(ColumnFamilyStore, AppendingByteBufInputPlus)}.
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
            FileMessageHeader header;
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
                    transferContext.session.receive(header, ssTableMultiWriter);
                }
            }
            catch (InterruptedException e)
            {
                // nop, thread was interrupted by the parent class (this is, or should be, normal/good/happy path)
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
                ctx.close();

                //StreamingInboundHandler.this.state = true;
                //FileUtils.closeQuietly(inputStream);
            }
        }
    }
}
