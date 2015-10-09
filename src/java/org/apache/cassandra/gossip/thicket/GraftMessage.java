package org.apache.cassandra.gossip.thicket;

public class GraftMessage extends ThicketMessage
{
    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.GRAFT;
    }
}
