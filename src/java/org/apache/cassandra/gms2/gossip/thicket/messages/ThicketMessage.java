package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.cassandra.io.ISerializer;

public abstract class ThicketMessage
{
    private final InetAddress treeRoot;

    private final Map<InetAddress, Integer> loadEstimate;

    //TODO: maybe find a better representation of the loadEstimate
    protected ThicketMessage(InetAddress treeRoot, Map<InetAddress, Integer> loadEstimate)
    {
        this.treeRoot = treeRoot;
        this.loadEstimate = loadEstimate;
    }

    public InetAddress getTreeRoot()
    {
        return treeRoot;
    }

    public Map<InetAddress, Integer> getLoadEstimate()
    {
        return loadEstimate;
    }

    public abstract MessageType getMessageType();

    public abstract ISerializer getSerializer();
}
