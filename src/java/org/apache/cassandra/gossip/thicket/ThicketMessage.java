package org.apache.cassandra.gossip.thicket;

import org.apache.cassandra.gossip.GossipMessageId;

public abstract class ThicketMessage
{
    private final GossipMessageId messageId;

    protected ThicketMessage(GossipMessageId messageId)
    {
        this.messageId = messageId;
    }

    public abstract ThicketMessageType getMessageType();
}
