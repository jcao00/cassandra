package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class DataMessage extends ThicketMessage
{
    public final InetAddress treeRoot;
    public final Object payload;
    public final String client;

    /**
     * The number of nodes this message has already been passed through on it's traversal
     * down the spanning tree.
     */
    public final int hopCount;

    public DataMessage(InetAddress sender, GossipMessageId messageId, InetAddress treeRoot, Object payload, String client, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoot = treeRoot;
        this.payload = payload;
        this.client = client;
        hopCount = 1;
    }

    public DataMessage(InetAddress sender, Collection<LoadEstimate> estimates, DataMessage message)
    {
        super(sender, message.messageId, estimates);
        treeRoot = message.treeRoot;
        payload = message.payload;
        client = message.client;
        hopCount = message.hopCount + 1;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(super.toString());
        sb.append(", treeRoot: ").append(treeRoot);
        sb.append(", client: ").append(client);
        sb.append(", payload: ").append(payload);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof DataMessage))
            return false;
        DataMessage msg = (DataMessage)o;
        return super.equals(o) &&
               treeRoot.equals(msg.treeRoot) &&
               client.equals(msg.client) &&
               payload.equals(msg.payload);
    }
}
