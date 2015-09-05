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
package org.apache.cassandra.gossip.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class HyParViewMessageSerializersTest
{
    static final int VERSION = MessagingService.current_version;
    static final int SEED = 231234237;
    static final String DC_1 = "dc1";
    static final String DC_2 = "dc2`";

    private Random random;
    private static InetAddress sender;
    private static InetAddress originator;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        sender = InetAddress.getByName("127.0.0.1");
        originator = InetAddress.getByName("127.0.0.2");
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

    @Test
    public void joinMessage() throws IOException
    {
        JoinMessage msg = new JoinMessage(generateMessageId(), sender, DC_1);
        roundTripSerialization(msg, JoinMessage.serializer);
    }

    private static <T extends HyParViewMessage> void roundTripSerialization(T msg, IVersionedSerializer<T> serializer) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer(1024);
        serializer.serialize(msg, out, VERSION);
        ByteBuffer buf = out.buffer();
        Assert.assertEquals(serializer.serializedSize(msg, VERSION), buf.remaining());

        DataInputBuffer in = new DataInputBuffer(buf, false);
        T result = serializer.deserialize(in, VERSION);
        Assert.assertEquals(msg, result);
    }

    @Test
    public void joinResponseMessage_WithoutDisconnect() throws IOException
    {
        JoinResponseMessage msg = new JoinResponseMessage(generateMessageId(), sender, DC_1, Optional.<GossipMessageId>empty());
        roundTripSerialization(msg, JoinResponseMessage.serializer);
    }

    @Test
    public void joinResponseMessage_WithDisconnect() throws IOException
    {
        JoinResponseMessage msg = new JoinResponseMessage(generateMessageId(), sender, DC_1, Optional.of(generateMessageId()));
        roundTripSerialization(msg, JoinResponseMessage.serializer);
    }

    @Test
    public void forwardJoinMessage() throws IOException
    {
        ForwardJoinMessage msg = new ForwardJoinMessage(generateMessageId(), sender, DC_1, originator, DC_2, 3, generateMessageId());
        roundTripSerialization(msg, ForwardJoinMessage.serializer);
    }

    @Test
    public void neighborRequestMessage_WithoutDisconnect() throws IOException
    {
        NeighborRequestMessage msg = new NeighborRequestMessage(generateMessageId(), sender, DC_1, NeighborRequestMessage.Priority.LOW,
                                                                2, Optional.<GossipMessageId>empty());
        roundTripSerialization(msg, NeighborRequestMessage.serializer);
    }

    @Test
    public void neighborResponseMessage_WithDisconnect() throws IOException
    {
        NeighborResponseMessage msg = new NeighborResponseMessage(generateMessageId(), sender, DC_1, NeighborResponseMessage.Result.ACCEPT,
                                                                2, Optional.of(generateMessageId()));
        roundTripSerialization(msg, NeighborResponseMessage.serializer);
    }

    @Test
    public void neighborResponseMessage_WithoutDisconnect() throws IOException
    {
        NeighborResponseMessage msg = new NeighborResponseMessage(generateMessageId(), sender, DC_1, NeighborResponseMessage.Result.DENY,
                                                                2, Optional.<GossipMessageId>empty());
        roundTripSerialization(msg, NeighborResponseMessage.serializer);
    }

    @Test
    public void neighborRequestMessage_WithDisconnect() throws IOException
    {
        NeighborRequestMessage msg = new NeighborRequestMessage(generateMessageId(), sender, DC_1, NeighborRequestMessage.Priority.HIGH,
                                                                2, Optional.of(generateMessageId()));
        roundTripSerialization(msg, NeighborRequestMessage.serializer);
    }

    @Test
    public void disconnectMessage() throws IOException
    {
        DisconnectMessage msg = new DisconnectMessage(generateMessageId(), sender, DC_1);
        roundTripSerialization(msg, DisconnectMessage.serializer);
    }
}
