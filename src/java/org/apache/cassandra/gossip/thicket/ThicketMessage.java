package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public abstract class ThicketMessage
{
    public final InetAddress sender;
    public final GossipMessageId messageId;
    public final Collection<LoadEstimate> estimates;

    protected ThicketMessage(InetAddress sender, GossipMessageId messageId, Collection<LoadEstimate> estimates)
    {
        this.sender = sender;
        this.messageId = messageId;
        this.estimates = estimates;
    }

    public abstract ThicketMessageType getMessageType();

    public String toString()
    {
        return String.format("%s, sender: %s, msgId: %s", getMessageType(), sender, messageId);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ThicketMessage))
            return false;
        ThicketMessage msg = (ThicketMessage)o;
        return sender.equals(msg.sender) &&
               messageId.equals(msg.messageId) &&
               estimates.equals(msg.estimates);
    }
}
