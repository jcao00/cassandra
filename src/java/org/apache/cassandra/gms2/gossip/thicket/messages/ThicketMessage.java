package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;

import org.apache.cassandra.io.ISerializer;

public abstract class ThicketMessage
{
    private final InetAddress treeRoot;
    private final byte[] loadEstimate;

    //TODO: maybe find a better representation of the loadEstimate
    protected ThicketMessage(InetAddress treeRoot, byte[] loadEstimate)
    {
        this.treeRoot = treeRoot;
        this.loadEstimate = loadEstimate;
    }

    public InetAddress getTreeRoot()
    {
        return treeRoot;
    }

    public abstract MessageType getMessageType();

    public abstract ISerializer getSerializer();
}
