package org.apache.cassandra.gossip.thicket;

import org.apache.cassandra.gossip.GossipMessageId;

public class PruneMessage extends ThicketMessage
{
    public PruneMessage(GossipMessageId messageId)
    {
        super(messageId);
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }
}
