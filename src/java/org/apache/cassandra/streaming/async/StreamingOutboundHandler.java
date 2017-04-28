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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ByteBufDataOutputPlus;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingUtils;
import org.apache.cassandra.streaming.compress.CompressionInfo;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.ChecksumType;

/**
 * A netty {@link ChannelHandler} responsible for transferring files. Has the ability to defer the transfer of an individual file
 * if, for example, the rate limiting throttle has been reached.
 *
 * This class extends from {@link ChannelDuplexHandler} in order to get the {@link #channelWritabilityChanged(ChannelHandlerContext)}
 * behavior. That function allows us to know when the channel's high water mark has been exceeded (as the channel's writablility
 * will have changed), and thus we can back off reading bytes from disk/writing them to the channel. The high water mark
 * functionality affects only non zero-copy transfers; see the package-level documentation for more details.
 */
public class StreamingOutboundHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingOutboundHandler.class);

    /**
     * Number of milliseconds to delay transferring the file when we couldn't get the permits from the rate limiter.
     */
    private static final int DELAY_TIME_MILLS = 200;

    /**
     * Overall state for this handler.
     */
    enum State { READY, PROCESSING, CLOSED }

    /**
     * The length of the first bytes of each new message/file transfer. Includes magic, stream message type id,
     * header length, and checksum of the length.
     */
    static final int MESSAGE_PREFIX_LENGTH = 13;

    /**
     * If a chunk cannot be sent immediately due to failure to acquire the permits from the {@link StreamRateLimiter},
     * this enum represents the state of attempting to acquite those permits.
     */
    private enum ThrottleState { START, AWAIT_GLOBAL_THROTTLE, AWAIT_INTER_DC_THROTTLE, ACQUIRED }

    private final StreamSession session;
    private final int protocolVersion;
    private final StreamRateLimiter rateLimiter;

    /**
     * A flag to indicate if the destination node is in the same datacenter.
     */
    private final boolean peerInSameDatacenter;

    /**
     * A mechanism to abstract out {@link StreamSession#progress(String, ProgressInfo.Direction, long, long)}.
     * This aides testing as setting up the mock dependencies of that function are non-trivial.
     */
    private final Consumer<ProgressUpdate> progressReporter;

    private State state;
    private CurrentMessage currentMessage;

    /**
     * Flag to indicate if the current channel is using SSL/TLS.
     */
    private boolean isSecure;

    private StreamCompressionSerializer serializer;

    private ScheduledFuture<?> scheduledWrite;

    public StreamingOutboundHandler(StreamSession session, int protocolVersion, StreamRateLimiter rateLimiter)
    {
        this (session, protocolVersion, rateLimiter, update -> session.progress(update.filename, update.direction, update.bytes, update.total));
    }

    @VisibleForTesting
    StreamingOutboundHandler(StreamSession session, int protocolVersion, StreamRateLimiter rateLimiter, Consumer<ProgressUpdate> progressReporter)
    {
        this.session = session;
        this.protocolVersion = protocolVersion;
        this.rateLimiter = rateLimiter;
        this.progressReporter = progressReporter;

        if (DatabaseDescriptor.getLocalDataCenter() != null && DatabaseDescriptor.getEndpointSnitch() != null)
            peerInSameDatacenter = DatabaseDescriptor.getLocalDataCenter().equals(
            DatabaseDescriptor.getEndpointSnitch().getDatacenter(session.peer));
        else
            peerInSameDatacenter = true;

        state = State.READY;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        isSecure = NettyFactory.isSecure(ctx.channel());
    }

    /**
     * {@inheritDoc}
     *
     * Responsible for serializing and sending {@link OutgoingFileMessage}s. The headers are serialized first,
     * and then the sstable is sent via {@link #transferFile(ChannelHandlerContext)}.
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) throws Exception
    {
        if (!canProcessMessage(message, promise))
            return;

        state = State.PROCESSING;

        try
        {
            if (message instanceof OutgoingFileMessage)
                sendFile(ctx, promise, (OutgoingFileMessage)message);
            else
                sendMessage(ctx, promise, (StreamMessage)message);
        }
        catch (Throwable t)
        {
            logger.debug("failed to send file", t);
            close();
            promise.tryFailure(t);

            // TODO:JEB review this error handling
//            if (currentMessage == null)
//                ofm.ref.release();
//            else
//                currentMessage.chunker.close();
        }
    }

    @VisibleForTesting
    boolean canProcessMessage(Object message, ChannelPromise promise)
    {
        if (state == State.CLOSED)
        {
            promise.tryFailure(new ClosedChannelException());
            return false;
        }
        if (state == State.PROCESSING)
        {
            promise.tryFailure(new IllegalStateException("currently processing an outbound message, but got another: " + message));
            return false;
        }
        else if (!(message instanceof StreamMessage))
        {
            promise.tryFailure(new UnsupportedMessageTypeException("message must be instance of OutgoingFileMessage"));
            return false;
        }

        return true;
    }

    /**
     * Serializes and sends a control {@link StreamMessage} down the channel.
     */
    @SuppressWarnings("unchecked")
    private void sendMessage(ChannelHandlerContext ctx, ChannelPromise promise, StreamMessage message) throws IOException
    {
        // we anticipate that the control messages are rather small, so allocating a ByteBuf shouldn't
        // blow out of memory.
        final IVersionedSerializer serializer = message.getSerializer();
        long messageSize = serializer.serializedSize(message, protocolVersion);
        if (messageSize > 1 << 30)
        {
            logger.error("something is seriously wrong with the calculated stream control message's size: {} bytes, type is {}", messageSize, message.getType());
            return;
        }

        int bufSize = (int)messageSize + MESSAGE_PREFIX_LENGTH;
        ByteBuf buf = ctx.alloc().buffer(bufSize, bufSize);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeByte(message.getType().getId());
        buf.writeInt((int)messageSize);
        buf.writeInt((int)ChecksumType.CRC32.of(ByteBuffer.allocate(4).putInt(0, (int)messageSize)));
        serializer.serialize(message, new ByteBufDataOutputPlus(buf), protocolVersion);

        state = State.READY;
        ctx.writeAndFlush(buf, promise);
    }

    private void sendFile(ChannelHandlerContext ctx, ChannelPromise promise, OutgoingFileMessage ofm) throws IOException
    {
        logger.debug("[Stream #{}] Sending {}", session.planId(), ofm);
        ofm.startTransfer();

        ByteBuf buf = null;
        try
        {
            buf = serializeHeader(ctx.alloc(), ofm.header, ofm.getType(), protocolVersion);
            ChannelFuture channelFuture = ctx.writeAndFlush(buf);
            channelFuture.addListener(future -> onHeaderSendComplete(future, ctx, promise));
        }
        catch (Exception e)
        {
            if (buf != null)
                buf.release();
            throw e;
        }

        CompressionInfo compressionInfo = ofm.header.getCompressionInfo();

        if (compressionInfo == null)
        {
            SstableChunker chunker = UncompressedSstableChunker.create(ctx.alloc(), ofm);
            long totalSize = StreamingUtils.totalSize(ofm.header.sections);
            currentMessage = new CurrentMessage(ofm, chunker, totalSize, promise, true);
        }
        else
        {
            SstableChunker chunker;
            if (isSecure)
                chunker = new SecureCompressedSstableChunker(ofm, compressionInfo, ctx.alloc());
            else
                chunker = new CompressedSstableChunker(ofm, compressionInfo);
            long totalSize = StreamingUtils.totalSize(compressionInfo.chunks);
            currentMessage = new CurrentMessage(ofm, chunker, totalSize, promise, false);
        }

        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     ofm.ref.get().getFilename(), session.peer, ofm.ref.get().getSSTableMetadata().repairedAt, currentMessage.totalSize);
        transferFile(ctx);

    }

    /**
     * Serializer the file message header. We send along the standard {@link MessagingService#PROTOCOL_MAGIC}
     * and a frame for the header so the recipient can check the validity of the message (via the PROTOCOL_MAGIC)
     * and then wait for the appropriate number of bytes for the header.
     *
     * Checksums are added for the length of the payload as well as the payload itself.
     */
    @VisibleForTesting
    static ByteBuf serializeHeader(ByteBufAllocator allocator, FileMessageHeader header, StreamMessage.Type type, int protocolVersion) throws IOException
    {
        ChecksumType checksum = ChecksumType.CRC32;
        final int msgSize = (int) FileMessageHeader.serializer.serializedSize(header, protocolVersion);
        final int totalSize = MESSAGE_PREFIX_LENGTH + msgSize + 4;
        ByteBuf buf = allocator.buffer(totalSize, totalSize);
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeByte(type.getId());
        buf.writeInt(msgSize);
        buf.writeInt((int)checksum.of(ByteBuffer.allocate(4).putInt(0, msgSize)));

        @SuppressWarnings("resource")
        ByteBufDataOutputPlus out = new ByteBufDataOutputPlus(buf);
        FileMessageHeader.serializer.serialize(header, out, protocolVersion);
        ByteBuffer payloadChecksumBuf = buf.nioBuffer(MESSAGE_PREFIX_LENGTH, msgSize);
        buf.writeInt((int) checksum.of(payloadChecksumBuf));

        assert buf.readableBytes() == totalSize;

        return buf;
    }

    void onHeaderSendComplete(Future<? super Void> future, ChannelHandlerContext ctx, ChannelPromise promise)
    {
        Throwable t = future.cause();
        // if the cause is null, sending the headers was successful
        if (t == null)
            return;

        logger.debug("failed to send file headers", t);
        close();
        promise.tryFailure(t);
    }

    /**
     * Transfers chunks of the target sstable. This method can, and probably will, be called mulitple times
     * for a given file as we'll need to delay processing for one of the following reasons:
     * <p>
     * 1) If the {@code ctx#channel} is not writable becasue we've hit the high water mark for
     * the number bytes allowed in the channel, this method will return without sending further chunks.
     * However, when outstanding chunks complete, {@link #onChunkComplete(Future, ChannelHandlerContext, int, boolean)}
     * is invoked, and it will call this method again if the {@code chunker} has more data to transfer.
     * <p>
     * 2) If we couldn't aqcuire the byte count of a chunk from the {@link StreamRateLimiter}, then the transfer is
     * suspended for some number of milliseconds, and attempted again.
     *
     * Note: this method handles all of it own exceptions, cleans up resources, and updates the channel appropriately.
     */
    private void transferFile(ChannelHandlerContext ctx)
    {
        if (state == State.CLOSED || currentMessage == null)
            return;

        try
        {
            // first, check to see if we can send any delayed chunk
            if (currentMessage.delayedChunk != null)
            {
                if (!ctx.channel().isWritable())
                    return;

                if (!trySendingChunk(ctx, currentMessage.delayedChunk, currentMessage.isLastChunk, currentMessage.throttleState))
                    return;
            }

            Iterator<Object> chunker = currentMessage.chunker;
            while (chunker.hasNext())
            {
                if (!ctx.channel().isWritable())
                    return;

                Object chunk = chunker.next();

                if (currentMessage.requiresCompression)
                {
                    if (serializer == null)
                        serializer = new StreamCompressionSerializer(NettyFactory.lz4Factory().fastCompressor(),
                                                                     NettyFactory.lz4Factory().fastDecompressor());
                    ByteBuf uncompressed = (ByteBuf) chunk;
                    ByteBuf compressed = serializer.serialize(uncompressed, protocolVersion);
                    uncompressed.release();
                    chunk = compressed;
                }
                boolean isLastChunk = !chunker.hasNext();
                if (!trySendingChunk(ctx, chunk, isLastChunk, ThrottleState.START))
                    return;
            }
        }
        catch (Throwable t)
        {
            logger.debug("failed to send file", t);
            ChannelPromise promise = close();
            if (promise != null)
                promise.tryFailure(t);
        }
    }

    /**
     * Attempt to get the permits from the rate limiter, and if successful send the chunk down the channel.
     * If the permits could not be acquired, we have to try again later: to do that we will *not* block the event loop
     * but reschedule processing for later.
     *
     * Note: as {@link RateLimiter#tryAcquire()}, used within {@link StreamRateLimiter}, does not indicate
     * the time duration until the permits become available, we have to take a SWAG and hope {@link #DELAY_TIME_MILLS}
     * is legit/good enough.
     */
    private boolean trySendingChunk(ChannelHandlerContext ctx, Object chunk, boolean isLastChunk, ThrottleState initialThrottleState)
    {
        int size = size(chunk);
        ThrottleState throttleState = tryAcquire(initialThrottleState, size);
        if (throttleState != ThrottleState.ACQUIRED)
        {
            currentMessage.setDealyedChunk(chunk, throttleState, isLastChunk);
            scheduledWrite = ctx.executor().schedule(() -> delayedTransfer(ctx), DELAY_TIME_MILLS, TimeUnit.MILLISECONDS);
            return false;
        }

        ChannelFuture channelFuture = ctx.writeAndFlush(chunk);

        // release the reference to the sstable as fast as possible
        if (isLastChunk)
            currentMessage.ofm.finishTransfer();
        channelFuture.addListener((future) -> onChunkComplete(future, ctx, size, isLastChunk));

        // onChunkComplete() can be invoked on the last chunk before from the writeAndFlush() above, which sets to currentMessage null
        if (currentMessage != null)
            currentMessage.removeDelayedChunk();
        return true;
    }

    /**
     * The method to be invoked when we need to delay file transfer; mainly, this cleans up rescheduling tidbits
     * before calling {@link #transferFile(ChannelHandlerContext)}.
     */
    private void delayedTransfer(ChannelHandlerContext ctx)
    {
        scheduledWrite = null;
        transferFile(ctx);
    }

    /**
     * Attempt to acquire the permits from the rate throttler.
     */
    private ThrottleState tryAcquire(ThrottleState beginningState, int messageSize)
    {
        switch (beginningState)
        {
            case START:
            case AWAIT_GLOBAL_THROTTLE:
                if (!rateLimiter.tryAcquireGlobal(messageSize))
                    return ThrottleState.AWAIT_GLOBAL_THROTTLE;
                // fall-through
            case AWAIT_INTER_DC_THROTTLE:
                if (!peerInSameDatacenter && !rateLimiter.tryAcquireInterDc(messageSize))
                    // it would ge great, but perhaps infeasible/unpractical, to return the acquired count back to
                    // the global limiter if we couldn't acquire here
                    return ThrottleState.AWAIT_INTER_DC_THROTTLE;
            case ACQUIRED:
                // huh, why bother calling if you've already got the permits ... just for safety sake, return success
                return ThrottleState.ACQUIRED;
        }

        return ThrottleState.ACQUIRED;
    }

    private int size(Object msg)
    {
        if (msg instanceof ByteBuf)
            return ((ByteBuf)msg).readableBytes();
        if (msg instanceof FileRegion)
            return (int) ((FileRegion) msg).count();
        return 0;
    }

    /**
     * A callback function invoked after each chunk of the sstable has been sent. A side effect of this method
     * is that on the first failure or the last success (for the given file being transfered) the {@code promise}
     * will be updated appropriately.
     *
     * @param future  The future for the result of transferring a chunk of an sstable.
     * @param ctx The current netty channel context.
     * @param size The size of the chunk.
     * @param isLastChunk indicator if this is the callback for the last chubnk of the file; if true,
     *                    resources can be cleaned up and released and the promise can be fulfilled as a success.
     * @return true if the chunk was sent; else false.
     */
    boolean onChunkComplete(Future<? super Void> future, ChannelHandlerContext ctx, int size, boolean isLastChunk)
    {
        if (state == State.CLOSED)
        {
            if (currentMessage != null && !currentMessage.promise.isDone())
                currentMessage.promise.cancel(false);
            return future.isSuccess();
        }
        else if (state == State.READY)
        {
            ChannelPromise promise = close();
            if (promise != null)
                promise.tryFailure(new AssertionError("shouldn't get to this state " +
                                                      "(state == READY, but we're still we're processing a chunk)"));
        }

        if (future.isSuccess())
        {
            if (currentMessage == null)
                return true;

            currentMessage.bytesSent += size;
            progressReporter.accept(new ProgressUpdate(currentMessage.fileName, ProgressInfo.Direction.OUT, currentMessage.bytesSent, currentMessage.totalSize));

            if (isLastChunk)
            {
                SSTableReader reader = currentMessage.ofm.ref.get();
                logger.debug("[Stream #{}] Finished streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                             reader.getFilename(), session.peer, reader.getSSTableMetadata().repairedAt, currentMessage.totalSize);
                state = State.READY;
                currentMessage.promise.trySuccess();
                currentMessage = null;
            }
            else
            {
                transferFile(ctx);
            }
            return true;
        }

        // if we got this far, we've failed in some way.
        ChannelPromise promise = close();
        if (promise != null)
            promise.tryFailure(future.cause());
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx)
    {
        ctx.fireChannelWritabilityChanged();

        // check that there's not a scheduled write already waiting. also, schedule the transferFile() invocation,
        // rather than executing it inline to avoid any crazy reentrant calls this method while transferFile() runs.
        if (ctx.channel().isWritable() && (scheduledWrite == null || scheduledWrite.isDone()))
        {
            ctx.executor().execute(() -> transferFile(ctx));
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx)
    {
        // ignore all calls to flush() as we handle all the semantics around flushing ourselves
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        ChannelPromise promise = close();
        if (promise != null)
            promise.tryFailure(new ClosedChannelException());

        ctx.fireChannelInactive();
    }

    private ChannelPromise close()
    {
        if (state == State.CLOSED)
            return null;

        state = State.CLOSED;

        if (currentMessage == null)
            return null;

        if (scheduledWrite != null)
        {
            scheduledWrite.cancel(false);
            scheduledWrite = null;
        }

        // TODO:JEB there's some file reference code to reconsider/clean up here
//        if (currentMessage.ofm != null)
//            currentMessage.ofm.finishTransfer();
        ChannelPromise promise = currentMessage.promise;
        currentMessage = null;

        return promise;
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise)
    {
        ChannelPromise messagePromise = close();
        if (messagePromise != null)
            messagePromise.tryFailure(new ClosedChannelException());
        ctx.close(promise);
    }

    // should only be used in testing
    @VisibleForTesting
    void setState(State s)
    {
        state = s;
    }

    // should only be used for testing
    @VisibleForTesting
    void setCurrentMessage(CurrentMessage msg)
    {
        this.currentMessage = msg;
    }

    /**
     * A simple struct to hold the state of the sstable being transerred.
     */
    static class CurrentMessage
    {
        final OutgoingFileMessage ofm;
        final Iterator<Object> chunker;
        final ChannelPromise promise;
        final String fileName;
        final long totalSize;
        final boolean requiresCompression;

        long bytesSent;
        Object delayedChunk;
        ThrottleState throttleState;
        boolean isLastChunk;

        CurrentMessage(OutgoingFileMessage ofm, Iterator<Object> chunker, long totalSize, ChannelPromise promise, boolean requiresCompression)
        {
            this.ofm = ofm;
            this.chunker = chunker;
            this.totalSize = totalSize;
            this.promise = promise;
            this.requiresCompression = requiresCompression;
            fileName = ofm.ref.get().descriptor.filenameFor(Component.DATA);
        }

        void setDealyedChunk(Object chunk, ThrottleState throttleState, boolean isLastChunk)
        {
            this.delayedChunk = chunk;
            this.throttleState = throttleState;
            this.isLastChunk = isLastChunk;
        }

        void removeDelayedChunk()
        {
            delayedChunk = null;
            throttleState = ThrottleState.START;
            isLastChunk = false;
        }
    }

    /**
     * Simple struct to encapsulate to arguments for {@link StreamSession#progress(String, ProgressInfo.Direction, long, long)}.
     */
    static class ProgressUpdate
    {
        private final String filename;
        private final ProgressInfo.Direction direction;
        private final long bytes;
        private final long total;

        ProgressUpdate(String filename, ProgressInfo.Direction direction, long bytes, long total)
        {
            this.filename = filename;
            this.direction = direction;
            this.bytes = bytes;
            this.total = total;
        }
    }
}