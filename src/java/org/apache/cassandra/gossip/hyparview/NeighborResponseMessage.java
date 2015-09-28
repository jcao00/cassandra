package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class NeighborResponseMessage extends HyParViewMessage
{
    public enum Result { ACCEPT, DENY }

    public final Result result;
    public final int neighborRequestsCount;

    public NeighborResponseMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, Result result,
                                   int neighborRequestsCount, HPVMessageId lastDisconnect)
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
}
