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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.Config.PROPERTY_PREFIX;

/**
 * A Netty {@link ChannelHandler} for serializing outbound messages.
 * <p>
 * On top of transforming a {@link QueuedMessage} into bytes, this handler also feeds back progress to the linked
 * {@link ChannelWriter} so that the latter can take decision on when data should be flushed (with and without coalescing).
 * See the javadoc on {@link ChannelWriter} for more details about the callbacks as well as message timeouts.
 *<p>
 * Note: this class derives from {@link ChannelDuplexHandler} so we can intercept calls to
 * {@link #userEventTriggered(ChannelHandlerContext, Object)} and {@link #channelWritabilityChanged(ChannelHandlerContext)}.
 *
 * // TODO:JEB add docs on how dataOutputPlus is used
 */
class MessageOutHandler extends ChannelDuplexHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MessageOutHandler.class);

    /**
     * The default size threshold for deciding when to auto-flush the channel.
     */
    private static final int DEFAULT_AUTO_FLUSH_THRESHOLD = 1 << 16;

    // reatining the pre 4.0 property name for backward compatibility.
    private static final String BUFFER_SIZE_PROPERTY = PROPERTY_PREFIX + "otc_buffer_size";
    static final int BUFFER_SIZE = Integer.getInteger(BUFFER_SIZE_PROPERTY, DEFAULT_AUTO_FLUSH_THRESHOLD);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int MESSAGE_PREFIX_SIZE = 12;

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The version of the messaging protocol we're communicating at.
     */
    private final int targetMessagingVersion;

    /**
     * The size of the backing buffer used in {@link ByteBufDataOutputStreamPlus}.
     */
    private final int bufferCapacity;

    private ByteBufDataOutputStreamPlus dataOutputPlus;

    private final ChannelWriter channelWriter;

    private final Supplier<QueuedMessage> backlogSupplier;

    MessageOutHandler(OutboundConnectionIdentifier connectionId, int targetMessagingVersion, ChannelWriter channelWriter, Supplier<QueuedMessage> backlogSupplier)
    {
        this (connectionId, targetMessagingVersion, channelWriter, backlogSupplier, BUFFER_SIZE);
    }

    MessageOutHandler(OutboundConnectionIdentifier connectionId, int targetMessagingVersion, ChannelWriter channelWriter,
                      Supplier<QueuedMessage> backlogSupplier, int bufferCapacity)
    {
        this.connectionId = connectionId;
        this.targetMessagingVersion = targetMessagingVersion;
        this.channelWriter = channelWriter;
        this.backlogSupplier = backlogSupplier;
        this.bufferCapacity = bufferCapacity;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        dataOutputPlus = ByteBufDataOutputStreamPlus.create(ctx, bufferCapacity);
    }

    /**
     * {@inheritDoc}
     *
     * //TODO:JEB comments
     */
    @Override
    public void write(ChannelHandlerContext ctx, Object o, ChannelPromise promise) throws IOException
    {
        try
        {
            if (!isMessageValid(o, promise))
                return;
            final QueuedMessage msg = (QueuedMessage)o;

            // chew through the backlog first as there are some application behaviors/flows
            // that will not work properly without correct ordering (repair)
            if (!msg.wasBacklogged)
            {
                while (true)
                {
                    QueuedMessage messageFromBacklog = backlogSupplier.get();
                    if (messageFromBacklog == null || !channelWriter.write(messageFromBacklog, false))
                        break;
                }
            }

            captureTracingInfo(msg);
            serializeMessage(msg, promise);
        }
        catch(Exception e)
        {
            exceptionCaught(ctx, e);
            promise.tryFailure(e);
        }
        finally
        {
            dataOutputPlus.resetPromises();
            // Make sure we signal the outChanel even in case of errors.
            if (channelWriter.onMessageProcessed(ctx))
                dataOutputPlus.flush();
        }
    }

    /**
     * Test to see if the message passed in is a {@link QueuedMessage} and if it has timed out or not. If the checks fail,
     * this method has the side effect of modifying the {@link ChannelPromise}.
     */
    boolean isMessageValid(Object o, ChannelPromise promise)
    {
        // optimize for the common case
        if (o instanceof QueuedMessage)
        {
            if (!((QueuedMessage)o).isTimedOut(System.nanoTime()))
            {
                return true;
            }
            else
            {
                promise.tryFailure(ExpiredException.INSTANCE);
            }
        }
        else
        {
            promise.tryFailure(new UnsupportedMessageTypeException(connectionId +
                                                                   " msg must be an instance of " + QueuedMessage.class.getSimpleName()));
        }
        return false;
    }

    /**
     * Record any tracing data, if enabled on this message.
     */
    @VisibleForTesting
    void captureTracingInfo(QueuedMessage msg)
    {
        try
        {
            byte[] sessionBytes = msg.message.parameters.get(Tracing.TRACE_HEADER);
            if (sessionBytes != null)
            {
                UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap(sessionBytes));
                TraceState state = Tracing.instance.get(sessionId);
                String message = String.format("Sending %s message to %s, size = %d bytes",
                                               msg.message.verb, connectionId.connectionAddress(),
                                               msg.message.serializedSize(targetMessagingVersion) + MESSAGE_PREFIX_SIZE);
                // session may have already finished; see CASSANDRA-5668
                if (state == null)
                {
                    byte[] traceTypeBytes = msg.message.parameters.get(Tracing.TRACE_TYPE);
                    Tracing.TraceType traceType = traceTypeBytes == null ? Tracing.TraceType.QUERY : Tracing.TraceType.deserialize(traceTypeBytes[0]);
                    Tracing.instance.trace(ByteBuffer.wrap(sessionBytes), message, traceType.getTTL());
                }
                else
                {
                    state.trace(message);
                    if (msg.message.verb == MessagingService.Verb.REQUEST_RESPONSE)
                        Tracing.instance.doneWithNonLocalSession(state);
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("{} failed to capture the tracing info for an outbound message, ignoring", connectionId, e);
        }
    }

    private void serializeMessage(QueuedMessage msg, ChannelPromise promise) throws IOException
    {
        dataOutputPlus.writeInt(MessagingService.PROTOCOL_MAGIC);
        dataOutputPlus.writeInt(msg.id);

        // int cast cuts off the high-order half of the timestamp, which we can assume remains
        // the same between now and when the recipient reconstructs it.
        dataOutputPlus.writeInt((int) NanoTimeToCurrentTimeMillis.convert(msg.timestampNanos));
        msg.message.serialize(dataOutputPlus, targetMessagingVersion);

        PromiseCombiner promiseCombiner = new PromiseCombiner();
        for (int i = 0; i < dataOutputPlus.promises.size(); i++)
            promiseCombiner.add((Future)dataOutputPlus.promises.get(i));
        promiseCombiner.finish(promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws IOException
    {
        if (channelWriter.onTriggeredFlush(ctx))
            dataOutputPlus.flush();
    }

    /**
     * {@inheritDoc}
     *
     * // TODO:JEB fix comment
     * When the channel becomes writable (assuming it was previously unwritable), try to eat through any backlogged messages
     * {@link #backlogSupplier}. As we're on the event loop when this is invoked, no one else can fill up the netty
     * {@link ChannelOutboundBuffer}, so we should be able to make decent progress chewing through the backlog
     * (assuming not large messages). Any messages messages written from {@link OutboundMessagingConnection} threads won't
     * be processed immediately; they'll be queued up as tasks, and once this function returns, those messages can begin
     * to be consumed.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws ClosedChannelException
    {
        if (!ctx.channel().isWritable())
        {
            logger.debug("flushing on channel unwritable");
            dataOutputPlus.onlyFlush();
            ctx.fireChannelWritabilityChanged();
        }

        // do not try to consume the backlog here as there's a pertty much zero chance of being at the end of message.
        // meaning, we are probably in the middle of another message, and serializing another message will corrupt the stream.
    }

    /**
     * {@inheritDoc}
     *
     * If we get an {@link IdleStateEvent} for the write path, we want to close the channel as we can't make progress.
     * That assumes, of course, that there's any outstanding bytes in the channel to write. We don't necesarrily care
     * about idleness (for example, gossip channels will be idle most of the time), but instead our concern is
     * the ability to make progress when there's work to be done.
     * <p>
     * Note: this is invoked on the netty event loop.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
    {
        if (evt instanceof IdleStateEvent && ((IdleStateEvent)evt).state() == IdleState.WRITER_IDLE)
        {
            ChannelOutboundBuffer cob = ctx.channel().unsafe().outboundBuffer();
            if (cob != null && cob.totalPendingWriteBytes() > 0)
            {
                ctx.channel().attr(ChannelWriter.PURGE_MESSAGES_CHANNEL_ATTR)
                   .compareAndSet(Boolean.FALSE, Boolean.TRUE);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof IOException)
            logger.trace("{} io error", connectionId, cause);
        else
            logger.warn("{} error", connectionId, cause);

        ctx.close();
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws IOException
    {
        dataOutputPlus.close();
        ctx.flush();
        ctx.close(promise);
    }

    /**
     * // TODO:JEB fix this comment
     *
     * A {@link DataOutputStreamPlus} implementation that is backed by a {@link ByteBuf} that is swapped out with another when full.
     * Each buffer has a {@link ChannelPromise} associated with it,
     * so the downstream progress of buffer (as it is flushed) can be observed. Each message will be serialized into one
     * or more buffers, and each of those buffer's promises are associated with the message. That way the message's promise
     * and be assocaited with the promises of the buffers into which it was serialized. Several messages may be packed into
     * the same buffer, so each of those messages will share the same promise.
     */
    private static class ByteBufDataOutputStreamPlus extends BufferedDataOutputStreamPlus
    {
        private final ChannelHandlerContext ctx;
        private final int bufferCapacity;

        /**
         * A collection of {@link ChannelPromise}s that should be associated with the current {@link QueuedMessage}
         * that is being serialized. These promises will be matched up with the promise that was passed in at
         * {@link MessageOutHandler#write(ChannelHandlerContext, Object, ChannelPromise)}, so that promise can be properly
         * notified when all the buffers (and their own respective promises) have been handled downstream.
         */
        private final List<ChannelPromise> promises;

        /**
         * The current {@link ByteBuf} that is being written to.
         */
        private ByteBuf buf;

        /**
         * A promise to be used with the current {@code buf} when calling
         * {@link ChannelHandlerContext#write(Object, ChannelPromise)}.
         */
        private ChannelPromise promise;

        ByteBufDataOutputStreamPlus(ChannelHandlerContext ctx, int bufferCapacity, ByteBuf buf, ByteBuffer buffer)
        {
            super(buffer);
            this.ctx = ctx;
            this.buf = buf;
            this.bufferCapacity = bufferCapacity;

            promises = new ArrayList<>(2);
            promise = ctx.newPromise();
            promises.add(promise);
        }

        static ByteBufDataOutputStreamPlus create(ChannelHandlerContext ctx, int bufferCapacity)
        {
            ByteBuf buf = ctx.alloc().directBuffer(bufferCapacity);
            ByteBuffer buffer = buf.nioBuffer(0, bufferCapacity);
            return new ByteBufDataOutputStreamPlus(ctx, bufferCapacity, buf, buffer);
        }

        @Override
        protected WritableByteChannel newDefaultChannel()
        {
            return new WritableByteChannel()
            {
                @Override
                public int write(ByteBuffer src) throws IOException
                {
                    assert src == buffer;
                    int size = src.position();
                    doFlush(size);
                    return size;
                }

                @Override
                public boolean isOpen()
                {
                    return channel.isOpen();
                }

                @Override
                public void close()
                {   }
            };
        }

        void writeAndMaybeFlush(boolean flush) throws ClosedChannelException
        {
            if (buffer.position() > 0)
            {
                if (!ctx.channel().isOpen())
                    throw new ClosedChannelException();

                int byteCount = buffer.position();
                buf.writerIndex(byteCount);

                if (flush)
                {
                    ctx.writeAndFlush(buf, promise);
                }
                else
                {
                    ctx.write(buf, promise);
                }

                buf = ctx.alloc().directBuffer(bufferCapacity);
                buffer = buf.nioBuffer(0, bufferCapacity);
                promise = ctx.newPromise();
                promises.add(promise);
            }
            else if (flush)
            {
                // we don't have any data in buffer, but we still want to flush anything that's in the channel
                ctx.flush();
            }
        }

        protected void onlyFlush()
        {
            ctx.flush();
        }

        /**
         * {@inheritDoc}
         *
         * @param count If greater than 0, it means {@link super#buffer} is full.
         *              If equal to 0, then an operational function, like {@link #flush()} or {@link #close()}
         *              was invoked on this {@link DataOutputStreamPlus}, not necessarily on the channel or
         *              channel handler (unless the owning {@link MessageOutHandler} calls those functions).
         */
        @Override
        protected void doFlush(int count) throws IOException
        {
            // if count == 0, flush() or close() was invoked. -in this
            // if count > 0, we are invoked because the backing buffer is full
            writeAndMaybeFlush(count == 0);
        }

        /**
         * //TODO:JEB clean up comment
         * This should be called when serializing a message is complete. If a the backing {@link super#buffer} is completely
         * filled up when this method is invoked, the next message will not need a reference to the current {@link #promise}
         * as none of the next message's bytes will be inserted into the current {@link super#buffer}. Thus, only add a reference
         * to the current {@link #promise} into {@link #promises} if there is space in the buffer
         */
        void resetPromises()
        {
            promises.clear();

            if (buffer.hasRemaining())
            {
                promises.add(promise);
            }
        }

        @Override
        public void close() throws IOException
        {
            if (ctx.channel().isOpen())
            {
                writeAndMaybeFlush(true);
            }
            else if (buffer.position() > 0)
            {
                // if there's unflushed data, and the channel is closed, fail the promise
                buf.release();
                promise.tryFailure(new ClosedChannelException());

                buf = null;
                buffer = null;
                promise = null;
                promises.clear();
            }
        }
    }
}
