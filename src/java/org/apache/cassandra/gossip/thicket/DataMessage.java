package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

import org.apache.cassandra.gossip.GossipMessageId;

public class DataMessage extends ThicketMessage
{
    public final InetAddress treeRoot;
    public final Object payload;
    public final String client;

    public DataMessage(InetAddress sender, GossipMessageId messageId, InetAddress treeRoot, Object payload, String client)
    {
        super(sender, messageId);
        this.treeRoot = treeRoot;
        this.payload = payload;
        this.client = client;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }
}
