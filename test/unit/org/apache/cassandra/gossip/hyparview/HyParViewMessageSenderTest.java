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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.DISCONNECT_SERIALIZER;
import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.FORWARD_JOIN_SERIALIZER;
import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.JOIN_RESPONSE_SERIALIZER;
import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.JOIN_SERIALIZER;
import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.NEIGHBOR_REQUEST_SERAILIZER;
import static org.apache.cassandra.gossip.hyparview.HyParViewMessageSender.NEIGHBOR_RESPONSE_SERIALIZER;

public class HyParViewMessageSenderTest
{
    static final int VERSION = MessagingService.current_version;
    static final int SEED = 231234237;
    static final String DC_1 = "dc1";
    static final String DC_2 = "dc2`";

    Random random;
    static InetAddress sender;
    static InetAddress originator;

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

    private HPVMessageId generateMessageId()
    {
        return new HPVMessageId.IdGenerator(random.nextInt()).generate();
    }

    @Test
    public void joinMessage() throws IOException
    {
        JoinMessage msg = new JoinMessage(generateMessageId(), sender, DC_1);
        roundTripSerialization(msg, JOIN_SERIALIZER);
    }

    private <T extends HyParViewMessage> void roundTripSerialization(T msg, IVersionedSerializer<T> serializer) throws IOException
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
        JoinResponseMessage msg = new JoinResponseMessage(generateMessageId(), sender, DC_1, Optional.<HPVMessageId>empty());
        roundTripSerialization(msg, JOIN_RESPONSE_SERIALIZER);
    }

    @Test
    public void joinResponseMessage_WithDisconnect() throws IOException
    {
        JoinResponseMessage msg = new JoinResponseMessage(generateMessageId(), sender, DC_1, Optional.of(generateMessageId()));
        roundTripSerialization(msg, JOIN_RESPONSE_SERIALIZER);
    }

    @Test
    public void forwardJoinMessage() throws IOException
    {
        ForwardJoinMessage msg = new ForwardJoinMessage(generateMessageId(), sender, DC_1, originator, DC_2, 3, generateMessageId());
        roundTripSerialization(msg, FORWARD_JOIN_SERIALIZER);
    }

    @Test
    public void neighborRequestMessage_WithoutDisconnect() throws IOException
    {
        NeighborRequestMessage msg = new NeighborRequestMessage(generateMessageId(), sender, DC_1, NeighborRequestMessage.Priority.LOW,
                                                                2, Optional.<HPVMessageId>empty());
        roundTripSerialization(msg, NEIGHBOR_REQUEST_SERAILIZER);
    }

    @Test
    public void neighborResponseMessage_WithDisconnect() throws IOException
    {
        NeighborResponseMessage msg = new NeighborResponseMessage(generateMessageId(), sender, DC_1, NeighborResponseMessage.Result.ACCEPT,
                                                                2, Optional.of(generateMessageId()));
        roundTripSerialization(msg, NEIGHBOR_RESPONSE_SERIALIZER);
    }

    @Test
    public void neighborResponseMessage_WithoutDisconnect() throws IOException
    {
        NeighborResponseMessage msg = new NeighborResponseMessage(generateMessageId(), sender, DC_1, NeighborResponseMessage.Result.DENY,
                                                                2, Optional.<HPVMessageId>empty());
        roundTripSerialization(msg, NEIGHBOR_RESPONSE_SERIALIZER);
    }

    @Test
    public void neighborRequestMessage_WithDisconnect() throws IOException
    {
        NeighborRequestMessage msg = new NeighborRequestMessage(generateMessageId(), sender, DC_1, NeighborRequestMessage.Priority.HIGH,
                                                                2, Optional.of(generateMessageId()));
        roundTripSerialization(msg, NEIGHBOR_REQUEST_SERAILIZER);
    }

    @Test
    public void disconnectMessage() throws IOException
    {
        DisconnectMessage msg = new DisconnectMessage(generateMessageId(), sender, DC_1);
        roundTripSerialization(msg, DISCONNECT_SERIALIZER);
    }
}
