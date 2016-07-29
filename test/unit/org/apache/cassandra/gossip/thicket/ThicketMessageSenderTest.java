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
package org.apache.cassandra.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class ThicketMessageSenderTest
{
    static final int VERSION = MessagingService.current_version;
    static final int SEED = 89123471;
    static BroadcastServiceClient client;

    Random random;
    static InetAddress sender;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        sender = InetAddress.getByName("127.0.0.1");
        client = new SimpleClient();
        ThicketMessageSender.addSerializer(client);
    }

    @Before
    public void setUp()
    {
        random = new Random(SEED);
    }

    private GossipMessageId generateMessageId()
    {
        return new GossipMessageId.IdGenerator(random.nextInt()).generate();
    }

    private static <T extends ThicketMessage> void roundTripSerialization(T msg, IVersionedSerializer<T> serializer) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer(1024);
        serializer.serialize(msg, out, VERSION);
        ByteBuffer buf = out.buffer();
        Assert.assertEquals(serializer.serializedSize(msg, VERSION), buf.remaining());

        DataInputBuffer in = new DataInputBuffer(buf, false);
        T result = serializer.deserialize(in, VERSION);
        Assert.assertEquals(msg, result);
    }

    private List<LoadEstimate> buildEstimates(int count) throws UnknownHostException
    {
        List<LoadEstimate> estimates = new LinkedList<>();
        for (int i = 0; i < count; i++)
            estimates.add(new LoadEstimate(InetAddress.getByName("127.1.0." + i), random.nextInt(7)));

        return estimates;
    }

    @Test
    public void dataMessage() throws IOException
    {
        DataMessage msg = new DataMessage(sender, generateMessageId(), InetAddress.getByName("127.0.0.12"), "ThisIaAPayload",
                                          client.getClientName(), buildEstimates(1));
        roundTripSerialization(msg, ThicketMessageSender.DATA_SERIALIZER);
    }

    @Test
    public void summaryMessage() throws IOException
    {
        Multimap<InetAddress, GossipMessageId> receivedMessages = HashMultimap.create();
        for (int i = 0; i < 7; i++)
        {
            InetAddress addr = InetAddress.getByName("127.0.1." + i);
            int msgs = random.nextInt(8);
            for (int j = 0; j < msgs; j++)
                receivedMessages.put(addr, generateMessageId());
        }

        SummaryMessage msg = new SummaryMessage(sender, generateMessageId(), receivedMessages, buildEstimates(2));
        roundTripSerialization(msg, ThicketMessageSender.SUMMARY_SERIALIZER);
    }

    @Test
    public void graftMessage() throws IOException
    {
        List<InetAddress> treeRoots = new LinkedList<>();
        for (int i = 0; i < 27; i++)
            treeRoots.add(InetAddress.getByName("127.0.1." + i));

        GraftMessage msg = new GraftMessage(sender, generateMessageId(), treeRoots, buildEstimates(3));
        roundTripSerialization(msg, ThicketMessageSender.GRAFT_SERIALIZER);
    }

    @Test
    public void pruneMessage() throws IOException
    {
        List<InetAddress> treeRoots = new LinkedList<>();
        for (int i = 0; i < 27; i++)
            treeRoots.add(InetAddress.getByName("127.0.1." + i));

        PruneMessage msg = new PruneMessage(sender, generateMessageId(), treeRoots, buildEstimates(4));
        roundTripSerialization(msg, ThicketMessageSender.PRUNE_SERIALIZER);
    }
}
