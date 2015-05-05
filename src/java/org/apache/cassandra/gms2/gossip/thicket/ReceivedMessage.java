package org.apache.cassandra.gms2.gossip.thicket;

import java.net.InetAddress;

public class ReceivedMessage
{
    final Object msgId;
    final InetAddress treeRoot;

    ReceivedMessage(Object msgId, InetAddress treeRoot)
    {
        this.msgId = msgId;
        this.treeRoot = treeRoot;
    }

    public int hashCode()
    {
        return 37 * msgId.hashCode() + treeRoot.hashCode();
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ReceivedMessage))
            return false;
        if (o == this)
            return true;
        ReceivedMessage msg = (ReceivedMessage)o;
        return msgId.equals(msg.msgId) && treeRoot.equals(msg.treeRoot);
    }
}
