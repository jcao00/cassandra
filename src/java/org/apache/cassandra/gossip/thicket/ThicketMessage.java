package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

import org.apache.cassandra.gossip.GossipMessageId;

public abstract class ThicketMessage
{
    public final InetAddress sender;
    public final GossipMessageId messageId;

    protected ThicketMessage(InetAddress sender, GossipMessageId messageId)
    {
        this.sender = sender;
        this.messageId = messageId;
    }

    public abstract ThicketMessageType getMessageType();
}
