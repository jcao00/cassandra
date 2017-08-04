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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.base.Charsets;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.MessageInHandler.MessageHeader;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;

public class MessageInHandlerTest
{
    private static final InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    private static final int MSG_VERSION = MessagingService.current_version;
    private static final long DEFAULT_MESSAGE_THRESHOLD = OutboundMessagingPool.LARGE_MESSAGE_THRESHOLD;

    private static final int MSG_ID = 42;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void channelRead_BadMagic() throws Exception
    {
        int len = MessageInHandler.FIRST_SECTION_BYTE_COUNT;
        buf = Unpooled.buffer(len, len);
        buf.writeInt(-1);
        buf.writerIndex(len);

        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, null, DEFAULT_MESSAGE_THRESHOLD);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        channel.writeInbound(buf);
        Assert.assertFalse(channel.isOpen());
    }

    @Test
    public void channelRead_HappyPath_NoParameters() throws Exception
    {
        MessageInWrapper result = channelRead_HappyPath(Collections.emptyMap(), DEFAULT_MESSAGE_THRESHOLD);
        Assert.assertTrue(result.messageIn.parameters.isEmpty());
    }

    @Test
    public void channelRead_HappyPath_WithParameters() throws Exception
    {
        Map<String, byte[]> parameters = new HashMap<>();
        parameters.put("p1", "val1".getBytes(Charsets.UTF_8));
        parameters.put("p2", "val2".getBytes(Charsets.UTF_8));
        MessageInWrapper result = channelRead_HappyPath(parameters, DEFAULT_MESSAGE_THRESHOLD);
        Assert.assertEquals(2, result.messageIn.parameters.size());
    }

    private MessageInWrapper channelRead_HappyPath(Map<String, byte[]> parameters, long messageSizeThreshold) throws Exception
    {
        MessageOut msgOut = new MessageOut(MessagingService.Verb.ECHO);
        for (Map.Entry<String, byte[]> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());
        serialize(msgOut);

        MessageInWrapper wrapper = new MessageInWrapper();
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, wrapper.messageConsumer, messageSizeThreshold);
        handler.channelRead(null, buf);

        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertEquals(MSG_ID, wrapper.id);
        Assert.assertEquals(msgOut.from, wrapper.messageIn.from);
        Assert.assertEquals(msgOut.verb, wrapper.messageIn.verb);

        return wrapper;
    }

    @Test
    public void channelRead_HappyPath_ChangeMode() throws Exception
    {
        GossipDigestSyn msg = new GossipDigestSyn("cluster", "partitioner", Collections.emptyList());
        MessageOut msgOut = new MessageOut(MessagingService.Verb.GOSSIP_DIGEST_SYN, msg, GossipDigestSyn.serializer);
        serialize(msgOut);

        MessageInWrapper wrapper = new MessageInWrapper();
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, wrapper.messageConsumer, 4);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertEquals(MessageInHandler.Mode.INLINE, handler.getMode());
        handler.channelRead(channel.pipeline().firstContext(), buf);

        Assert.assertTrue(wrapper.latch.await(1000, TimeUnit.MILLISECONDS));

        // assert about the message
        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertEquals(MSG_ID, wrapper.id);
        Assert.assertEquals(msgOut.from, wrapper.messageIn.from);
        Assert.assertEquals(msgOut.verb, wrapper.messageIn.verb);

        // now assert state of handler
        Assert.assertEquals(MessageInHandler.Mode.OFFLOAD, handler.getMode());
        Assert.assertNull(handler.getMessageHeader());

        // now, publish a seconds message, just to make sure things are still correct
        wrapper.reset();
        serialize(msgOut);
        handler.channelRead(channel.pipeline().firstContext(), buf);

        Assert.assertTrue(wrapper.latch.await(1000, TimeUnit.MILLISECONDS));
        Assert.assertNotNull(wrapper.messageIn);

        handler.close();
    }

    private void serialize(MessageOut msgOut) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(MSG_ID); // this is the id
        buf.writeInt((int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime()));

        msgOut.serialize(new ByteBufDataOutputPlus(buf), MSG_VERSION);
    }

    @Test
    public void channelRead_WithHalfReceivedParameters() throws Exception
    {
        MessageOut msgOut = new MessageOut(MessagingService.Verb.ECHO);
        msgOut = msgOut.withParameter("p3", "val1".getBytes(Charsets.UTF_8));

        serialize(msgOut);

        int originalWriterIndex = buf.writerIndex();
        ByteBuf firstChunk = buf.retainedSlice(0, originalWriterIndex - 6);

        MessageInWrapper wrapper = new MessageInWrapper();
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, wrapper.messageConsumer, DEFAULT_MESSAGE_THRESHOLD);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        handler.channelRead(channel.pipeline().firstContext(), firstChunk);

        Assert.assertNull(wrapper.messageIn);

        MessageHeader header = handler.getMessageHeader();
        Assert.assertEquals(MSG_ID, header.messageId);
        Assert.assertEquals(msgOut.verb, header.verb);
        Assert.assertEquals(msgOut.from, header.from);

        ByteBuf secondChunk = buf.retainedSlice(originalWriterIndex, 6);
        handler.channelRead(channel.pipeline().firstContext(), secondChunk);
        Assert.assertNotNull(wrapper.messageIn);
    }

    @Test
    public void canReadNextParam_HappyPath() throws IOException
    {
        buildParamBuf(13);
        Assert.assertTrue(MessageInHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_OnlyFirstByte() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(1);
        Assert.assertFalse(MessageInHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_PartialUTF() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(5);
        Assert.assertFalse(MessageInHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_TruncatedValueLength() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(buf.writerIndex() - 13 - 2);
        Assert.assertFalse(MessageInHandler.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_MissingLastBytes() throws IOException
    {
        buildParamBuf(13);
        buf.writerIndex(buf.writerIndex() - 2);
        Assert.assertFalse(MessageInHandler.canReadNextParam(buf));
    }

    private void buildParamBuf(int valueLength) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        ByteBufDataOutputPlus output = new ByteBufDataOutputPlus(buf);
        output.writeUTF("name");
        byte[] array = new byte[valueLength];
        output.writeInt(array.length);
        output.write(array);
    }

    @Test
    public void exceptionHandled()
    {
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, null, DEFAULT_MESSAGE_THRESHOLD);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        handler.exceptionCaught(channel.pipeline().firstContext(), new EOFException());
        Assert.assertFalse(channel.isOpen());
    }

    @Test
    public void channelRead_INLINE()
    {
        channelRead(MessageInHandler.Mode.INLINE);
    }

    @Test
    public void channelRead_OFFLOAD()
    {
        channelRead(MessageInHandler.Mode.OFFLOAD);
    }

    private void channelRead(MessageInHandler.Mode m)
    {
        final int threshold = 8;
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, null, threshold);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        handler.setMode(m);
        handler.setQueuedBuffers(new RebufferingByteBufDataInputPlus(10, 100, channel.config()));

        int bufSize = m == MessageInHandler.Mode.INLINE ? threshold - 1 : threshold + 1;
        buf = Unpooled.buffer(bufSize);
        buf.writerIndex(bufSize);
        handler.channelRead(null, buf);

        int expectedQueuedCount = m == MessageInHandler.Mode.INLINE ? 0 : bufSize;
        Assert.assertEquals(expectedQueuedCount, handler.getQueuedBuffers().available());
    }

    @Test
    public void channelRead_INLINE_RetainedBuffer()
    {
        MessageInHandler handler = new MessageInHandler(addr.getAddress(), MSG_VERSION, null, DEFAULT_MESSAGE_THRESHOLD);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        ByteBuf buf1 = channel.alloc().buffer(1);
        buf1.writerIndex(1);
        handler.channelRead(channel.pipeline().firstContext(), buf1);
        ByteBuf retained = handler.getRetainedInlineBuffer();
        Assert.assertNotNull(retained);
        Assert.assertEquals(1, retained.readableBytes());

        ByteBuf buf2 = channel.alloc().buffer(1);
        buf2.writerIndex(1);
        handler.channelRead(channel.pipeline().firstContext(), buf2);
        retained = handler.getRetainedInlineBuffer();
        Assert.assertNotNull(retained);
        Assert.assertEquals(2, retained.readableBytes());

        // buf1's refCnt should be 1 as the cumulator used in channelRead() can simply copy over buf2's bytes because buf1's
        // maxCapacity, by default, should be large
        Assert.assertEquals(1, buf1.refCnt());
        Assert.assertEquals(0, buf2.refCnt());

        handler.close();
        Assert.assertEquals(0, retained.refCnt());
    }

    private static class MessageInWrapper
    {
        volatile MessageIn messageIn;
        volatile int id;

        // for tests that are multithreaded
        CountDownLatch latch = new CountDownLatch(1);

        final BiConsumer<MessageIn, Integer> messageConsumer = (messageIn, integer) ->
        {
            this.messageIn = messageIn;
            this.id = integer;
            latch.countDown();
        };

        public void reset()
        {
            messageIn = null;
            id = -1;
            latch = new CountDownLatch(1);
        }
    }
}
