package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

import org.apache.cassandra.gossip.GossipMessageId;

public class DataMessage extends ThicketMessage
{
    public final Object payload;
    public final InetAddress sender;
    public final InetAddress originator;
    public final String client;

    public DataMessage(InetAddress sender, GossipMessageId messageId, Object payload, InetAddress originator, String client)
    {
        super(messageId);
        this.payload = payload;
        this.sender = sender;
        this.originator = originator;
        this.client = client;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }
}
