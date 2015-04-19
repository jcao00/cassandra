package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.io.ISerializer;

public class GraftRequestMessage extends ThicketMessage
{
    private final InetAddress treeRoot;

    public GraftRequestMessage(InetAddress treeRoot, Map<InetAddress, Integer> loadEstimate)
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
        return MessageType.GRAFT_REQUEST;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
