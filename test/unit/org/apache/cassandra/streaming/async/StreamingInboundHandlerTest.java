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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.net.async.AppendingByteBufInputPlus;
import org.apache.cassandra.net.async.AppendingByteBufInputPlusTest.TestChannelConfig;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ChecksumType;

@Ignore("JEB to fix this up")
public class StreamingInboundHandlerTest extends CQLTester
{
    private static final int VERSION = StreamSession.CURRENT_VERSION;
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 0);
    private static final ChecksumType checksum = ChecksumType.CRC32;

    private static final int SESSION_INDEX = 0;

    private StreamingInboundHandler handler;
    private AppendingByteBufInputPlus inputPlus;
    private ByteBuf buf;
    private ChannelHandlerContext ctx;
    private String table;

    @Before
    public void setup()
    {
        if (table == null)
            table = createTable("CREATE TABLE %s (k int PRIMARY KEY, t int)");

        inputPlus = new AppendingByteBufInputPlus(new TestChannelConfig());
        handler = new StreamingInboundHandler(REMOTE_ADDR, VERSION);
        handler.setPendingBuffers(inputPlus);
        ctx = new TestChannelHandlerContext();
    }

    @After
    public void tearDown()
    {
        if (buf != null)
        {
            while (buf.refCnt() > 0)
                buf.release();
        }
    }

//    @Test
//    public void parseBufferedBytes_LittleBufferedData() throws IOException
//    {
//        buf = Unpooled.buffer(2, 2);
//        buf.writerIndex(2);
//        inputPlus.append(buf);
//        handler.parseBufferedBytes(ctx);
//        Assert.assertEquals(State.START, handler.getState());
//    }
//
//    @Test
//    public void parseBufferedBytes_SimpleHappyPath() throws IOException
//    {
//        StreamResultFuture future = StreamResultFuture.initReceivingSide(SESSION_INDEX, UUID.randomUUID(), "testStream",
//                                                                         REMOTE_ADDR.getAddress(), REMOTE_ADDR.getAddress(),
//                                                                         ctx.channel(), true, false, UUID.randomUUID());
//        FileMessageHeader header = createHeader(future, table);
//        buf = StreamingOutboundHandler.serializeHeader(UnpooledByteBufAllocator.DEFAULT, header, StreamMessage.Type.FILE, VERSION);
//        inputPlus.append(buf);
//        handler.parseBufferedBytes(ctx);
//        Assert.assertEquals(State.FILE_TRANSFER_PAYLOAD, handler.getState());
//        FileTransferContext ctx = handler.getCurrentTransferContext();
//        Assert.assertEquals(header, ctx.header);
//    }

//    private FileMessageHeader createHeader(StreamResultFuture future, String table)
//    {
//        List<Pair<Long, Long>> sections = new ArrayList<>();
//        sections.add(Pair.create(25L, 50L));
//        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
//        SerializationHeader serializationHeader = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
//        FileMessageHeader header = new FileMessageHeader(metadata.id, future.planId,
//                                                         SESSION_INDEX, 1, BigFormat.latestVersion, SSTableFormat.Type.BIG,
//                                                         100, sections, (CompressionMetadata) null, System.currentTimeMillis(), 0, serializationHeader.toComponent());
//        return header;
//    }

//    @Test
//    public void parseBufferedBytes_MultipleBuffersHappyPath() throws IOException
//    {
//        StreamResultFuture future = StreamResultFuture.initReceivingSide(SESSION_INDEX, UUID.randomUUID(), "testStream",
//                                                                         REMOTE_ADDR.getAddress(), REMOTE_ADDR.getAddress(),
//                                                                         ctx.channel(), true, false, UUID.randomUUID());
//        FileMessageHeader header = createHeader(future, table);
//        buf = StreamingOutboundHandler.serializeHeader(UnpooledByteBufAllocator.DEFAULT, header, StreamMessage.Type.FILE, VERSION);
//
//        Assert.assertEquals(State.START, handler.getState());
//        inputPlus.append(buf.readRetainedSlice(MESSAGE_PREFIX_LENGTH - 1));
//        handler.parseBufferedBytes(ctx);
//        Assert.assertEquals(State.START, handler.getState());
//
//        inputPlus.append(buf.readRetainedSlice(5));
//        handler.parseBufferedBytes(ctx);
//        Assert.assertEquals(State.FILE_TRANSFER_HEADER_PAYLOAD, handler.getState());
//
//        inputPlus.append(buf);
//        handler.parseBufferedBytes(ctx);
//        Assert.assertEquals(State.FILE_TRANSFER_PAYLOAD, handler.getState());
//        FileTransferContext ctx = handler.getCurrentTransferContext();
//        Assert.assertEquals(header, ctx.header);
//    }
//
//    @Test (expected = ChecksumMismatchException.class)
//    public void parseBufferedBytes_CorruptHeaderLength() throws IOException
//    {
//        buf = Unpooled.buffer(MESSAGE_PREFIX_LENGTH, MESSAGE_PREFIX_LENGTH);
//        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
//        buf.writeByte(1);
//        final int length = 42;
//        buf.writeInt(length);
//        int positionToCorrupt = buf.writerIndex() - 2;
//        int b = buf.getByte(positionToCorrupt);
//        b = (b & 0x01) == 0 ? b + 1 : b - 1;
//        buf.setByte(positionToCorrupt, b);
//        buf.writeInt((int)checksum.of(ByteBuffer.allocate(4).putInt(0, length)));
//
//        inputPlus.append(buf);
//        handler.parseBufferedBytes(ctx);
//    }
//
//    @Test (expected = ChecksumMismatchException.class)
//    public void parseBufferedBytes_CorruptHeaders() throws IOException
//    {
//        StreamResultFuture future = StreamResultFuture.initReceivingSide(SESSION_INDEX, UUID.randomUUID(), "testStream",
//                                                                         REMOTE_ADDR.getAddress(), REMOTE_ADDR.getAddress(),
//                                                                         ctx.channel(), true, false, UUID.randomUUID());
//        FileMessageHeader header = createHeader(future, table);
//        buf = StreamingOutboundHandler.serializeHeader(UnpooledByteBufAllocator.DEFAULT, header, StreamMessage.Type.FILE, VERSION);
//        int positionToCorrupt = buf.writerIndex() - 2;
//        int b = buf.getByte(positionToCorrupt);
//        b = (b & 0x01) == 0 ? b + 1 : b - 1;
//        buf.setByte(positionToCorrupt, b);
//        inputPlus.append(buf);
//        handler.parseBufferedBytes(ctx);
//    }

    private static class TestChannelHandlerContext implements ChannelHandlerContext
    {
        private Channel channel = new TestChannel();

        public Channel channel()
        {
            return channel;
        }

        public EventExecutor executor()
        {
            return null;
        }

        public String name()
        {
            return null;
        }

        public ChannelHandler handler()
        {
            return null;
        }

        public boolean isRemoved()
        {
            return false;
        }

        public ChannelHandlerContext fireChannelRegistered()
        {
            return null;
        }

        public ChannelHandlerContext fireChannelUnregistered()
        {
            return null;
        }

        public ChannelHandlerContext fireChannelActive()
        {
            return null;
        }

        public ChannelHandlerContext fireChannelInactive()
        {
            return null;
        }

        public ChannelHandlerContext fireExceptionCaught(Throwable cause)
        {
            return null;
        }

        public ChannelHandlerContext fireUserEventTriggered(Object evt)
        {
            return null;
        }

        public ChannelHandlerContext fireChannelRead(Object msg)
        {
            return null;
        }

        public ChannelHandlerContext fireChannelReadComplete()
        {
            return null;
        }

        public ChannelHandlerContext fireChannelWritabilityChanged()
        {
            return null;
        }

        public ChannelFuture bind(SocketAddress localAddress)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress)
        {
            return null;
        }

        public ChannelFuture disconnect()
        {
            return null;
        }

        public ChannelFuture close()
        {
            return null;
        }

        public ChannelFuture deregister()
        {
            return null;
        }

        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture disconnect(ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture close(ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture deregister(ChannelPromise promise)
        {
            return null;
        }

        public ChannelHandlerContext read()
        {
            return null;
        }

        public ChannelFuture write(Object msg)
        {
            return null;
        }

        public ChannelFuture write(Object msg, ChannelPromise promise)
        {
            return null;
        }

        public ChannelHandlerContext flush()
        {
            return null;
        }

        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture writeAndFlush(Object msg)
        {
            return null;
        }

        public ChannelPromise newPromise()
        {
            return null;
        }

        public ChannelProgressivePromise newProgressivePromise()
        {
            return null;
        }

        public ChannelFuture newSucceededFuture()
        {
            return null;
        }

        public ChannelFuture newFailedFuture(Throwable cause)
        {
            return null;
        }

        public ChannelPromise voidPromise()
        {
            return null;
        }

        public ChannelPipeline pipeline()
        {
            return null;
        }

        public ByteBufAllocator alloc()
        {
            return null;
        }

        public <T> Attribute<T> attr(AttributeKey<T> key)
        {
            return null;
        }

        public <T> boolean hasAttr(AttributeKey<T> key)
        {
            return false;
        }
    }

    private static class TestChannel implements Channel
    {
        private static final AtomicInteger idGenerator = new AtomicInteger();

        ChannelConfig config = new TestChannelConfig();
        private ChannelId channelId = new TestChannelId(idGenerator.getAndIncrement());

        public ChannelId id()
        {
            return channelId;
        }

        public EventLoop eventLoop()
        {
            return null;
        }

        public Channel parent()
        {
            return null;
        }

        public ChannelConfig config()
        {
            return config;
        }

        public boolean isOpen()
        {
            return false;
        }

        public boolean isRegistered()
        {
            return false;
        }

        public boolean isActive()
        {
            return false;
        }

        public ChannelMetadata metadata()
        {
            return null;
        }

        public SocketAddress localAddress()
        {
            return null;
        }

        public SocketAddress remoteAddress()
        {
            return null;
        }

        public ChannelFuture closeFuture()
        {
            return null;
        }

        public boolean isWritable()
        {
            return false;
        }

        public long bytesBeforeUnwritable()
        {
            return 0;
        }

        public long bytesBeforeWritable()
        {
            return 0;
        }

        public Unsafe unsafe()
        {
            return null;
        }

        public ChannelPipeline pipeline()
        {
            return null;
        }

        public ByteBufAllocator alloc()
        {
            return null;
        }

        public ChannelFuture bind(SocketAddress localAddress)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress)
        {
            return null;
        }

        public ChannelFuture disconnect()
        {
            return null;
        }

        public ChannelFuture close()
        {
            return null;
        }

        public ChannelFuture deregister()
        {
            return null;
        }

        public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture disconnect(ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture close(ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture deregister(ChannelPromise promise)
        {
            return null;
        }

        public Channel read()
        {
            return null;
        }

        public ChannelFuture write(Object msg)
        {
            return null;
        }

        public ChannelFuture write(Object msg, ChannelPromise promise)
        {
            return null;
        }

        public Channel flush()
        {
            return null;
        }

        public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise)
        {
            return null;
        }

        public ChannelFuture writeAndFlush(Object msg)
        {
            return null;
        }

        public ChannelPromise newPromise()
        {
            return null;
        }

        public ChannelProgressivePromise newProgressivePromise()
        {
            return null;
        }

        public ChannelFuture newSucceededFuture()
        {
            return null;
        }

        public ChannelFuture newFailedFuture(Throwable cause)
        {
            return null;
        }

        public ChannelPromise voidPromise()
        {
            return null;
        }

        public <T> Attribute<T> attr(AttributeKey<T> key)
        {
            return null;
        }

        public <T> boolean hasAttr(AttributeKey<T> key)
        {
            return false;
        }

        public int compareTo(Channel o)
        {
            return 0;
        }
    }

    private static class TestChannelId implements ChannelId
    {
        private final int id;

        private TestChannelId(int id)
        {
            this.id = id;
        }

        public String asShortText()
        {
            return String.valueOf(id);
        }

        public String asLongText()
        {
            return String.valueOf(id);
        }

        public int compareTo(ChannelId o)
        {
            if (o instanceof TestChannelId)
                return this.id - ((TestChannelId)o).id;
            return 1;
        }
    }
}
