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
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.net.async.TestScheduledFuture;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

public class NettyStreamingMessageSenderTest
{
    private static final int VERSION = StreamMessage.CURRENT_VERSION;
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 0);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 0);

    private EmbeddedChannel channel;
    private StreamSession session;
    private NettyStreamingMessageSender sender;
    private NettyStreamingMessageSender.FileStreamTask fileStreamTask;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        channel = new EmbeddedChannel();
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.FALSE);
        session = new StreamSession(REMOTE_ADDR.getAddress(), REMOTE_ADDR.getAddress(), (connectionId, protocolVersion) -> null, 0, true, true, UUID.randomUUID());
        StreamResultFuture future = StreamResultFuture.initReceivingSide(0, UUID.randomUUID(), "desc", REMOTE_ADDR.getAddress(), channel, true, true, UUID.randomUUID());
        session.init(future);
        OutboundConnectionIdentifier connectionId = OutboundConnectionIdentifier.stream(LOCAL_ADDR, REMOTE_ADDR);
        sender = new NettyStreamingMessageSender(session, connectionId, (cid, protocolVersion) -> null, VERSION);
    }

    @After
    public void tearDown()
    {
        if (fileStreamTask != null)
            fileStreamTask.unsetChannel();
    }

    @Test
    public void KeepAliveTask_normalSend()
    {
        Assert.assertTrue(channel.isOpen());
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.run();
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void KeepAliveTask_channelClosed()
    {
        channel.close();
        Assert.assertFalse(channel.isOpen());
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());
        task.run();
        Assert.assertTrue(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void KeepAliveTask_closed()
    {
        Assert.assertTrue(channel.isOpen());
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());

        sender.setClosed();
        Assert.assertFalse(sender.connected());
        task.run();
        Assert.assertTrue(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
    }


    @Test
    public void KeepAliveTask_CurrentlyStreaming()
    {
        Assert.assertTrue(channel.isOpen());
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.TRUE);
        NettyStreamingMessageSender.KeepAliveTask task = sender.new KeepAliveTask(channel, session);
        task.future = new TestScheduledFuture();
        Assert.assertFalse(task.future.isCancelled());

        Assert.assertTrue(sender.connected());
        task.run();
        Assert.assertFalse(task.future.isCancelled());
        Assert.assertFalse(channel.releaseOutbound());
    }

    @Test
    public void FileStreamTask_acquirePermit_closed()
    {
        fileStreamTask = sender.new FileStreamTask(null);
        sender.setClosed();
        Assert.assertFalse(fileStreamTask.acquirePermit(1));
    }

    @Test
    public void FileStreamTask_acquirePermit_HapppyPath()
    {
        int permits = sender.semaphoreAvailablePermits();
        fileStreamTask = sender.new FileStreamTask(null);
        Assert.assertTrue(fileStreamTask.acquirePermit(1));
        Assert.assertEquals(permits - 1, sender.semaphoreAvailablePermits());
    }

    @Test
    public void FileStreamTask_BadChannelAttr()
    {
        int permits = sender.semaphoreAvailablePermits();
        channel.attr(NettyStreamingMessageSender.TRANSFERRING_FILE_ATTR).set(Boolean.TRUE);
        fileStreamTask = sender.new FileStreamTask(null);
        fileStreamTask.injectChannel(channel);
        fileStreamTask.run();
        Assert.assertEquals(StreamSession.State.FAILED, session.state());
        Assert.assertFalse(channel.releaseOutbound());
        Assert.assertEquals(permits, sender.semaphoreAvailablePermits());
    }

    @Test
    public void FileStreamTask_HappyPath()
    {
        int permits = sender.semaphoreAvailablePermits();
        fileStreamTask = sender.new FileStreamTask(new CompleteMessage());
        fileStreamTask.injectChannel(channel);
        fileStreamTask.run();
        Assert.assertNotEquals(StreamSession.State.FAILED, session.state());
        Assert.assertTrue(channel.releaseOutbound());
        Assert.assertEquals(permits, sender.semaphoreAvailablePermits());
    }
}
