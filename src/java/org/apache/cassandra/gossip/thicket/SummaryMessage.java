package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import com.google.common.collect.Multimap;

import org.apache.cassandra.gossip.GossipMessageId;

public class SummaryMessage extends ThicketMessage
{
    public final Multimap<InetAddress, GossipMessageId> receivedMessages;
    public final Collection<LoadEstimate> estimates;

    public SummaryMessage(InetAddress sender, GossipMessageId messageId, Multimap<InetAddress, GossipMessageId> receivedMessages, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId);
        this.receivedMessages = receivedMessages;
        this.estimates = estimates;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.SUMMARY;
    }
}
