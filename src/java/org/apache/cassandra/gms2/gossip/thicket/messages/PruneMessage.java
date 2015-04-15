package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;

import org.apache.cassandra.io.ISerializer;

public class PruneMessage extends ThicketMessage
{
    private final InetAddress treeRoot;

    public PruneMessage(InetAddress treeRoot, byte[] loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.treeRoot = treeRoot;
    }

    public InetAddress getTreeRoot()
    {
        return treeRoot;
    }

    public MessageType getMessageType()
    {
        return MessageType.PRUNE;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
