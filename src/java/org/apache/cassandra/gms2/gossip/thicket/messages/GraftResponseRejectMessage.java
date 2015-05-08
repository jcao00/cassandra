package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.io.ISerializer;

public class GraftResponseRejectMessage extends ThicketMessage
{
    private final int attemptCount;
    private final String clientId;

    /**
     * In case of REJECT, a node may offer an alternative peer that can be contacted for the requestor for grafting.
     */
    private final InetAddress graftAlternate;

    public GraftResponseRejectMessage(InetAddress treeRoot, String clientId, int attemptCount, InetAddress graftAlternate, Map<InetAddress, Integer> loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.attemptCount = attemptCount;
        this.graftAlternate = graftAlternate;
        this.clientId = clientId;
    }

    public GraftResponseRejectMessage(GraftRequestMessage request, InetAddress graftAlternate, Map<InetAddress, Integer> loadEstimate)
    {
        super(request.getTreeRoot(), loadEstimate);
        this.attemptCount = request.getAttemptCount();
        this.graftAlternate = graftAlternate;
        this.clientId = request.getClientId();
    }

    public String getClientId()
    {
        return clientId;
    }

    public int getAttemptCount()
    {
        return attemptCount;
    }

    public InetAddress getGraftAlternate()
    {
        return graftAlternate;
    }

    public MessageType getMessageType()
    {
        return MessageType.GRAFT_RESPONSE_REJECT;
    }

    public ISerializer getSerializer()
    {
        return null;
    }

}
