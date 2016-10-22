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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.FastThreadLocalThread;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.AppendingByteBufInputStream;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.CompressedInputStream;
import org.apache.cassandra.streaming.compress.CompressedStreamReader;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;

/**
 * Handles inbound file transfers.
 */
public class StreamingReceiveHandler extends ChannelInboundHandlerAdapter
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingReceiveHandler.class);

    private static final int MAX_BUFFERED_BYTES = 1 << 22;
    private final InetSocketAddress remoteAddress;
    private final int protocolVersion;

    /**
     * A queue for the incoming {@link ByteBuf}s, which will be processed by the {@link #blockingIOThread}.
     */
    private AppendingByteBufInputStream inputStream;

    /**
     * The background thread that blocks for data.
     */
    private Thread blockingIOThread;

    private volatile boolean closed;

    public StreamingReceiveHandler(InetSocketAddress remoteAddress, int protocolVersion)
    {
        this.remoteAddress = remoteAddress;
        this.protocolVersion = protocolVersion;
    }

    @Override
    @SuppressWarnings("resource")
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        inputStream = new AppendingByteBufInputStream(MAX_BUFFERED_BYTES, MAX_BUFFERED_BYTES, ctx);
//        blockingIOThread = new Thread(new DeserializingSstableTask(ctx, inputStream, remoteAddress, protocolVersion),
//                                      String.format("Stream-Receive-%s", remoteAddress.toString()));
//        blockingIOThread.setDaemon(true);
//        blockingIOThread.start();
        blockingIOThread = new FastThreadLocalThread(new DeserializingSstableTask(ctx, inputStream, remoteAddress, protocolVersion),
                                      String.format("Stream-Receive-%s", remoteAddress.toString()));
        blockingIOThread.setDaemon(true);
        blockingIOThread.start();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (!closed && msg instanceof ByteBuf)
        {
            try
            {
                inputStream.append((ByteBuf) msg);
                if (inputStream.getBufferredByteCount() > MAX_BUFFERED_BYTES)
                {
                    ctx.channel().config().setAutoRead(false);
                }
            }
            catch(IllegalStateException ise)
            {
                closed = true;
                ReferenceCountUtil.release(msg);
            }
        }
        else
        {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
        ctx.fireChannelInactive();
    }

    void close()
    {
        if (closed)
            return;

        closed = true;
        if (blockingIOThread != null)
            blockingIOThread.interrupt();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("exception occurred while in processing streaming file", cause);
        close();
        ctx.fireExceptionCaught(cause);
    }

//    /**
//     * A task that can execute the blocking deserialization behavior of {@link StreamReader#read(ReadableByteChannel)}.
//     */
//    private class DeserializingSstableTask implements Runnable
//    {
//        private final ChannelHandlerContext ctx;
//        private final InputStream inputStream;
//        private final InetSocketAddress remoteAddress;
//        private final int protocolVersion;
//
//        DeserializingSstableTask(ChannelHandlerContext ctx, InputStream inputStream, InetSocketAddress remoteAddress, int protocolVersion)
//        {
//            this.ctx = ctx;
//            this.inputStream = inputStream;
//            this.remoteAddress = remoteAddress;
//            this.protocolVersion = protocolVersion;
//        }
//
//        @Override
//        public void run()
//        {
//            FileMessageHeader header = null;
//            StreamSession session = null;
//            try
//            {
//                while (!closed)
//                {
//                    header = readHeader(inputStream);
//                    session = StreamManager.instance.findSession(remoteAddress.getAddress(), new IncomingFileMessage(null, header));
//                    if (session == null)
//                        throw new IllegalArgumentException("could not locate session for file header data: " + header);
//
//                    StreamReader reader = header.compressionInfo == null
//                                          ? new StreamReader(header, session)
//                                          : new CompressedStreamReader(header, session);
//
//
//                    SSTableMultiWriter ssTableMultiWriter = reader.read(Channels.newChannel(inputStream));
//                    session.receive(new IncomingFileMessage(ssTableMultiWriter, header));
//                }
//            }
//            catch (EOFException e)
//            {
//                // thrown when netty socket closes/is interrupted
//                logger.debug("eof reading from socket; closing");
//            }
//            catch (Throwable t)
//            {
//                // Throwable can be Runtime error containing IOException.
//                // In that case we don't want to retry.
//                // TODO:JEB resolve this
////                Throwable cause = t;
////                while ((cause = cause.getCause()) != null)
////                {
////                    if (cause instanceof IOException)
////                        throw (IOException) cause;
////                }
////                JVMStabilityInspector.inspectThrowable(t);
//                logger.error("failed in streambackground thread", t);
//            }
//            finally
//            {
//
//                // TODO:JEB do we close the session or send compelte somewheres?
//
//                StreamingReceiveHandler.this.closed = true;
//                FileUtils.closeQuietly(inputStream);
//            }
//        }
//
//        /**
//         * Parse the incoming {@link ByteBuf} for the file headers.
//         *
//         * @return The derserialized file headers if there were enough bytes in the buffer; else, null.
//         */
//        private FileMessageHeader readHeader(InputStream in) throws IOException
//        {
//            DataInputPlus input = new DataInputPlus.DataInputStreamPlus(in);
//            int magic = input.readInt();
//            MessagingService.validateMagic(magic);
//
//            // message size is not currently used, but if we move to a non-blocking paradigm for the individual file transfers,
//            // then we would need it (at least for the headers)
//            int msgSize = input.readInt();
//
//            return FileMessageHeader.serializer.deserialize(input, protocolVersion);
//        }
//    }
}