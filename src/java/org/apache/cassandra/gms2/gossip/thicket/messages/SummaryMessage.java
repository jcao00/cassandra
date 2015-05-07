package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.gms2.gossip.thicket.ReceivedMessage;
import org.apache.cassandra.io.ISerializer;

public class SummaryMessage extends ThicketMessage
{
    private final String clientId;

    private final Set<ReceivedMessage> receivedMessages;

    public SummaryMessage(InetAddress treeRoot, String clientId, Set<ReceivedMessage> receivedMessages, Map<InetAddress, Integer>loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.clientId = clientId;
        this.receivedMessages = receivedMessages;
    }

    public String getClientId()
    {
        return clientId;
    }

    public Set<ReceivedMessage> getReceivedMessages()
    {
        return receivedMessages;
    }

    public MessageType getMessageType()
    {
        return MessageType.SUMMARY;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
