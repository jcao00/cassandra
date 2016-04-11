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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.StreamRateLimiter;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.StreamingOutboundHandler.CurrentMessage;
import org.apache.cassandra.streaming.async.StreamingOutboundHandler.ProgressUpdate;
import org.apache.cassandra.streaming.async.StreamingOutboundHandler.State;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Pair;

public class StreamingOutboundHandlerTest extends CQLTester
{
    private static final InetAddress PEER;
    private static final InetAddress CONNECTING;

    private static final Consumer<ProgressUpdate> PROGRESS_SINK = update -> {};

    static
    {
        DatabaseDescriptor.daemonInitialization();

        try
        {
            PEER = InetAddress.getByName("127.0.0.2");
            CONNECTING = InetAddress.getByName("127.0.0.1");
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private boolean inited;
    private SSTableReader sstable;
    private TableMetadata metadata;
    private StreamSession session;
    private StreamingOutboundHandler handler;
    private EmbeddedChannel channel;

    @Before
    public void setUp() throws Throwable
    {
        if (!inited)
        {
            String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, c1 int)");
            metadata = Schema.instance.getTableMetadata(KEYSPACE, table);

            for (int i = 0; i < 16; i++)
                execute("INSERT INTO %s (id, c1) VALUES (?, ?)", i, 42 * i);

            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            cfs.forceBlockingFlush();
            sstable = cfs.getLiveSSTables().iterator().next();
            inited = true;
        }

        session = new StreamSession(PEER, CONNECTING, 1, true, true, null);
        handler = new StreamingOutboundHandler(session, StreamSession.CURRENT_VERSION, StreamRateLimiter.instance, PROGRESS_SINK);
        channel = new EmbeddedChannel(handler);
    }

    @Test
    public void onChunkComplete_ChunkSent_Success()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        CurrentMessage msg = new CurrentMessage(createOFM(), new TestChunker(), sstable.getDataChannel().size(), promise, false);
        handler.setCurrentMessage(msg);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), 0, false);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(!promise.isDone());
    }

    @Test
    public void onChunkComplete_ChunkSent_Fail()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        Throwable cause = new NullPointerException();
        future.setFailure(cause);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        CurrentMessage msg = new CurrentMessage(createOFM(), new TestChunker(), sstable.getDataChannel().size(), promise, false);
        handler.setCurrentMessage(msg);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), 0, true);

        Assert.assertFalse(chunkSent);
        Assert.assertTrue(promise.isDone());
        Assert.assertTrue(!promise.isSuccess());
        Assert.assertEquals(cause, promise.cause());
    }

    private OutgoingFileMessage createOFM()
    {
        List<Pair<Long, Long>> sections = new ArrayList<>();
        sections.add(Pair.create(0L, sstable.getDataChannel().size()));
        return new OutgoingFileMessage(sstable.ref(), session, 1, 10, sections, System.currentTimeMillis());
    }

    @Test
    public void onChunkComplete_LastChunkSent_Success()
    {
        handler.setState(State.PROCESSING);
        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        ChannelPromise promise = new DefaultChannelPromise(channel);

        CurrentMessage msg = new CurrentMessage(createOFM(), new TestChunker(), sstable.getDataChannel().size(), promise, false);
        handler.setCurrentMessage(msg);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), 0, true);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(promise.isSuccess());
    }

    @Test
    public void onChunkComplete_Stopped_ChunkSent()
    {
        handler.setState(State.CLOSED);

        DefaultChannelPromise future = (DefaultChannelPromise)channel.newPromise();
        future.setSuccess();
        ChannelPromise promise = new DefaultChannelPromise(channel);
        CurrentMessage msg = new CurrentMessage(createOFM(), new TestChunker(), sstable.getDataChannel().size(), promise, false);
        handler.setCurrentMessage(msg);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), 0, true);

        Assert.assertTrue(chunkSent);
        Assert.assertTrue(promise.isCancelled());
    }

    @Test
    public void onChunkComplete_Stopped_ChunkNotSent()
    {
        handler.setState(State.CLOSED);

        ChannelFuture future = channel.newPromise();
        future.cancel(false);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        CurrentMessage msg = new CurrentMessage(createOFM(), new TestChunker(), sstable.getDataChannel().size(), promise, false);
        handler.setCurrentMessage(msg);
        boolean chunkSent = handler.onChunkComplete(future, channel.pipeline().firstContext(), 0, true);

        Assert.assertFalse(chunkSent);
        Assert.assertTrue(promise.isCancelled());
    }

    @Test
    public void canProcessMessage_Good()
    {
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertTrue(handler.canProcessMessage(ofm, promise));
        Assert.assertFalse(promise.isDone());
    }

    @Test
    public void canProcessMessage_ProcessingState()
    {
        handler.setState(State.PROCESSING);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertFalse(handler.canProcessMessage(ofm, promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    @Test
    public void canProcessMessage_DoneState()
    {
        handler.setState(State.CLOSED);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        OutgoingFileMessage ofm = new OutgoingFileMessage();
        Assert.assertFalse(handler.canProcessMessage(ofm, promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    @Test
    public void canProcessMessage_InstanceTypeWrong()
    {
        handler.setState(State.PROCESSING);
        ChannelPromise promise = new DefaultChannelPromise(channel);
        Assert.assertFalse(handler.canProcessMessage(new CompleteMessage(UUID.randomUUID(), 42), promise));
        Assert.assertTrue(promise.isDone());
        Assert.assertFalse(promise.isSuccess());
    }

    private static class TestChunker implements CloseableIterator<Object>
    {
        public boolean hasNext()
        {
            return false;
        }

        public Object next()
        {
            return null;
        }

        public Iterator<Object> iterator()
        {
            return this;
        }

        public void close()
        {

        }
    }
}
