package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Multimap;

import org.apache.cassandra.gossip.GossipMessageId;

public class SummaryMessage extends ThicketMessage
{
    public final Multimap<InetAddress, GossipMessageId> receivedMessages;

    public SummaryMessage(InetAddress sender, GossipMessageId messageId, Multimap<InetAddress, GossipMessageId> receivedMessages, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.receivedMessages = receivedMessages;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.SUMMARY;
    }

    public String toString()
    {
        return String.format("%s, receivedMessages: %s", super.toString(), receivedMessages);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof SummaryMessage))
            return false;
        SummaryMessage msg = (SummaryMessage)o;
        return super.equals(o) && receivedMessages.equals(msg.receivedMessages);
    }
}
