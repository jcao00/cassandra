package org.apache.cassandra.gossip.thicket;

public abstract class ThicketMessage
{
    public abstract ThicketMessageType getMessageType();
}
