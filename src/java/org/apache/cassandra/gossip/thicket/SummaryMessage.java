package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.gossip.GossipMessageId;

public class SummaryMessage extends ThicketMessage
{
    public final InetAddress sender;
    public final List<ThicketService.TimestampedMessageId> receivedMessages;

    public SummaryMessage(InetAddress sender, GossipMessageId messageId, List<ThicketService.TimestampedMessageId> receivedMessages)
    {
        super(messageId);
        this.sender = sender;
        this.receivedMessages = receivedMessages;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.SUMMARY;
    }
}
