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
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ChannelWriter.CoalescingChannelWriter;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;

/**
 * with the write_Coalescing_* methods, if there's data in the channel.unsafe().outboundBuffer()
 * it means that there's something in the channel that hasn't yet been flushed to the transport (socket).
 * once a flush occurs, there will be an entry in EmbeddedChannel's outboundQueue. those two facts are leveraged in these tests.
 */
public class ChannelWriterTest
{
    private static final int COALESCE_WINDOW_MS = 10;

    private EmbeddedChannel channel;
    private ChannelWriter channelWriter;
    private NonSendingOutboundMessagingConnection omc;
    private Optional<CoalescingStrategy> coalescingStrategy;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.small(new InetSocketAddress("127.0.0.1", 0),
                                                                             new InetSocketAddress("127.0.0.2", 0));
        channel = new EmbeddedChannel();
        channelWriter = ChannelWriter.create(channel, Optional.empty());
        channel.pipeline().addFirst(new MessageOutHandler(id, MessagingService.current_version, channelWriter));
        omc = new NonSendingOutboundMessagingConnection(id, null, Optional.empty());
        coalescingStrategy = CoalescingStrategies.newCoalescingStrategy(CoalescingStrategies.Strategy.FIXED.name(), COALESCE_WINDOW_MS, null, "test");
    }

    @Test
    public void create_nonCoalescing()
    {
        Assert.assertSame(ChannelWriter.SimpleChannelWriter.class, ChannelWriter.create(channel, Optional.empty()).getClass());
    }

    @Test
    public void create_Coalescing()
    {
        Assert.assertSame(CoalescingChannelWriter.class, ChannelWriter.create(channel, coalescingStrategy).getClass());
    }

    @Test
    public void write_Coalescing_LostRaceForFlushTask()
    {
        CoalescingChannelWriter channelWriter = resetEnvForCoalescing(DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages());
        channelWriter.scheduledFlush.set(true);
        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() == 0);
        channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), 42), omc);
        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() > 0);
        Assert.assertFalse(channel.releaseOutbound());
        Assert.assertTrue(channelWriter.scheduledFlush.get());
    }

    @Test
    public void write_Coalescing_HitMinMessageCountForImmediateCoalesce()
    {
        CoalescingChannelWriter channelWriter = resetEnvForCoalescing(1);

        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() == 0);
        Assert.assertFalse(channelWriter.scheduledFlush.get());
        channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), 42), omc);

        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() == 0);
        Assert.assertTrue(channel.releaseOutbound());
        Assert.assertFalse(channelWriter.scheduledFlush.get());
    }

    @Test
    public void write_Coalescing_ScheduleFlushTask()
    {
        CoalescingChannelWriter channelWriter = resetEnvForCoalescing(DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages());

        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() == 0);
        Assert.assertFalse(channelWriter.scheduledFlush.get());
        channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), 42), omc);

        Assert.assertTrue(channelWriter.scheduledFlush.get());
        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() > 0);
        Assert.assertTrue(channelWriter.scheduledFlush.get());

        // this unfortunately know a little too much about how the sausage is made in CoalescingChannelWriter :-/
        channel.runScheduledPendingTasks();
        channel.runPendingTasks();
        Assert.assertTrue(channel.unsafe().outboundBuffer().totalPendingWriteBytes() == 0);
        Assert.assertFalse(channelWriter.scheduledFlush.get());
        Assert.assertTrue(channel.releaseOutbound());
    }

    private CoalescingChannelWriter resetEnvForCoalescing(int minMessagesForCoalesce)
    {
        channel = new EmbeddedChannel();
        CoalescingChannelWriter cw = new CoalescingChannelWriter(channel, coalescingStrategy.get(), minMessagesForCoalesce);
        channel.pipeline().addFirst(new ChannelOutboundHandlerAdapter()
        {
            public void flush(ChannelHandlerContext ctx) throws Exception
            {
                cw.onTriggeredFlush(ctx);
            }
        });
        omc.setChannelWriter(cw);
        return cw;
    }

    @Test
    public void writeBacklog_Empty()
    {
        BlockingQueue<QueuedMessage> queue = new LinkedBlockingQueue<>();
        Assert.assertEquals(0, channelWriter.writeBacklog(queue, omc, false));
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void writeBacklog_NotEmpty()
    {
        BlockingQueue<QueuedMessage> queue = new LinkedBlockingQueue<>();
        int count = 12;
        for (int i = 0; i < count; i++)
            queue.offer(new QueuedMessage(new MessageOut<>(ECHO), i));
        Assert.assertEquals(count, channelWriter.writeBacklog(queue, omc, false));
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void close()
    {
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertTrue(channel.isOpen());
        channelWriter.close();
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channelWriter.isClosed());
    }

    @Test
    public void softClose()
    {
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertTrue(channel.isOpen());
        channelWriter.softClose();
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channelWriter.isClosed());
    }

    @Test
    public void handleMessagePromise_FutureIsCancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        channelWriter.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1), omc, true);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
    }

    @Test
    public void handleMessagePromise_ExpiredException_DoNotRetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new ExpiredException());

        channelWriter.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1), omc, true);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(1, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessagePromise_NonIOException()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new NullPointerException("this is a test"));
        channelWriter.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1), omc, true);
        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessagePromise_IOException_ChannelNotClosed_RetryMsg()
    {
        ChannelPromise promise = channel.newPromise();
        promise.setFailure(new IOException("this is a test"));
        Assert.assertTrue(channel.isActive());
        channelWriter.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), omc, true);

        Assert.assertFalse(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertTrue(omc.sendMessageInvoked);
    }

    @Test
    public void handleMessagePromise_Cancelled()
    {
        ChannelPromise promise = channel.newPromise();
        promise.cancel(false);
        Assert.assertTrue(channel.isActive());
        channelWriter.handleMessageFuture(promise, new QueuedMessage(new MessageOut<>(ECHO), 1, 0, true, true), omc, true);

        Assert.assertTrue(channel.isActive());
        Assert.assertEquals(1, omc.getCompletedMessages().longValue());
        Assert.assertEquals(0, omc.getDroppedMessages().longValue());
        Assert.assertFalse(omc.sendMessageInvoked);
    }

    private static class NonSendingOutboundMessagingConnection extends OutboundMessagingConnection
    {
        boolean sendMessageInvoked;

        NonSendingOutboundMessagingConnection(OutboundConnectionIdentifier connectionId, EncryptionOptions.ServerEncryptionOptions encryptionOptions, Optional<CoalescingStrategy> coalescingStrategy)
        {
            super(connectionId, encryptionOptions, coalescingStrategy, new AllowAllInternodeAuthenticator());
        }

        @Override
        boolean sendMessage(QueuedMessage queuedMessage)
        {
            sendMessageInvoked = true;
            return true;
        }
    }
}
