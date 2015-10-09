package org.apache.cassandra.gossip.thicket;

public class PruneMessage extends ThicketMessage
{
    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }
}
