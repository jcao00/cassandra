package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class GraftMessage extends ThicketMessage
{
    public final Collection<InetAddress> treeRoots;

    public GraftMessage(GossipMessageId messageId, Collection<InetAddress> treeRoots)
    {
        super(messageId);
        this.treeRoots = treeRoots;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.GRAFT;
    }
}
