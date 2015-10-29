package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class PruneMessage extends ThicketMessage
{
    public final Collection<InetAddress> treeRoots;

    public PruneMessage(InetAddress sender, GossipMessageId messageId, Collection<InetAddress> treeRoots, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoots = treeRoots;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }

    public String toString()
    {
        return String.format("%s, treeRoots: %s", super.toString(), treeRoots);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof PruneMessage))
            return false;
        PruneMessage msg = (PruneMessage)o;
        return super.equals(o) && treeRoots.equals(msg.treeRoots);
    }
}
