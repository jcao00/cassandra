package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.io.ISerializer;

public class SummaryMessage extends ThicketMessage
{
    private final String clientId;

    private final Object summary;

    public SummaryMessage(InetAddress treeRoot, String clientId, Object summary, Map<InetAddress, Integer>loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.clientId = clientId;
        this.summary = summary;
    }

    public String getClientId()
    {
        return clientId;
    }

    public Object getSummary()
    {
        return summary;
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
