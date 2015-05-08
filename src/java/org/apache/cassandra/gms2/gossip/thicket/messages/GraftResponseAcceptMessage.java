package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.io.ISerializer;

public class GraftResponseAcceptMessage extends ThicketMessage
{
    private final String clientId;

    GraftResponseAcceptMessage(InetAddress treeRoot, String clientId, Map<InetAddress, Integer> loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.clientId = clientId;
    }

    public GraftResponseAcceptMessage(GraftRequestMessage msg, Map<InetAddress, Integer> inetAddressIntegerMap)
    {
        super(msg.getTreeRoot(), inetAddressIntegerMap);
        this.clientId = msg.getClientId();
    }

    public String getClientId()
    {
        return clientId;
    }

    public MessageType getMessageType()
    {
        return MessageType.GRAFT_RESPONSE_ACCEPT;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
