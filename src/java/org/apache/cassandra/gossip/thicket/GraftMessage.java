package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class GraftMessage extends ThicketMessage
{
    public final Collection<InetAddress> treeRoots;

    public GraftMessage(InetAddress sender, GossipMessageId messageId, Collection<InetAddress> treeRoots, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoots = treeRoots;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.GRAFT;
    }

    public String toString()
    {
        return String.format("%s, treeRoots: %s", super.toString(), treeRoots);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof GraftMessage))
            return false;
        GraftMessage msg = (GraftMessage)o;
        return super.equals(o) && treeRoots.equals(msg.treeRoots);
    }
}
