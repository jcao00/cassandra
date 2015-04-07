package org.apache.cassandra.gms2.gossip.peersampling;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetAddress;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.peersampling.messages.Disconnect;
import org.apache.cassandra.gms2.gossip.peersampling.messages.ForwardJoin;
import org.apache.cassandra.gms2.gossip.peersampling.messages.Join;
import org.apache.cassandra.gms2.gossip.peersampling.messages.JoinAck;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborRequest;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborResponse;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class HyParViewMessagesSerializerTest
{
    @Test
    public void joinSerialization() throws IOException
    {
        Join msg = new Join();
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof Join);
        Join msg2 = (Join)o;
        Assert.assertEquals(msg.getMessageType(), msg2.getMessageType());
    }

    @Test
    public void forwardJoinSerialization() throws IOException
    {
        ForwardJoin msg = new ForwardJoin(InetAddress.getByName("127.0.0.1"), (byte)3, (byte)6, (byte)4);
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof ForwardJoin);
        ForwardJoin ret = (ForwardJoin)o;
        Assert.assertEquals(msg.originator, ret.originator);
        Assert.assertEquals(msg.getMessageType(), ret.getMessageType());
    }

    @Test
    public void joinAckSerialization() throws IOException
    {
        JoinAck msg = new JoinAck();
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof JoinAck);
        JoinAck msg2 = (JoinAck)o;
        Assert.assertEquals(msg.getMessageType(), msg2.getMessageType());
    }

    @Test
    public void disconnectSerialization() throws IOException
    {
        Disconnect msg = new Disconnect();
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof Disconnect);
        Disconnect msg2 = (Disconnect)o;
        Assert.assertEquals(msg.getMessageType(), msg2.getMessageType());
    }

    @Test
    public void neighborRequestSerialization_HighPriority() throws IOException
    {
        neighborRequestSerialization(NeighborRequest.Priority.HIGH);
    }

    @Test
    public void neighborRequestSerialization_LowPriority() throws IOException
    {
        neighborRequestSerialization(NeighborRequest.Priority.LOW);
    }

    public void neighborRequestSerialization(NeighborRequest.Priority priority) throws IOException
    {
        NeighborRequest msg = new NeighborRequest(priority);
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof NeighborRequest);
        NeighborRequest msg2 = (NeighborRequest)o;
        Assert.assertEquals(msg.getPriority(), msg2.getPriority());
        Assert.assertEquals(priority, msg2.getPriority());
        Assert.assertEquals(msg.getMessageType(), msg2.getMessageType());
    }

    @Test
    public void neighborResponseSerialization_Accept() throws IOException
    {
        neighborResponseSerialization(NeighborResponse.Result.ACCEPT);
    }

    @Test
    public void neighborResponseSerialization_Reject() throws IOException
    {
        neighborResponseSerialization(NeighborResponse.Result.REJECT);
    }

    private void neighborResponseSerialization(NeighborResponse.Result result) throws IOException
    {
        NeighborResponse msg = new NeighborResponse(result);
        DataOutputBuffer buf = new DataOutputBuffer();
        msg.getSerializer().serialize(msg, buf);
        Object o = msg.getSerializer().deserialize(ByteStreams.newDataInput(buf.getData()));

        Assert.assertTrue(o instanceof NeighborResponse);
        NeighborResponse msg2 = (NeighborResponse)o;
        Assert.assertEquals(msg.getResult(), msg2.getResult());
        Assert.assertEquals(result, msg2.getResult());
        Assert.assertEquals(msg.getMessageType(), msg2.getMessageType());
    }
}
