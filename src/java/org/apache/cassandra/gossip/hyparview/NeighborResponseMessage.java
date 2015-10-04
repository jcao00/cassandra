package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public class NeighborResponseMessage extends HyParViewMessage
{
    public enum Result { ACCEPT, DENY }

    public final Result result;
    public final int neighborRequestsCount;

    public NeighborResponseMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, Result result,
                                   int neighborRequestsCount, Optional<HPVMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
        this.result = result;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_RESPONSE;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(200);
        sb.append(super.toString());
        sb.append(", result ").append(result);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof NeighborResponseMessage))
            return false;
        NeighborResponseMessage msg = (NeighborResponseMessage)o;

        return result.equals(msg.result) && neighborRequestsCount == msg.neighborRequestsCount;
    }
}
