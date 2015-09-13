package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class NeighborResponseMessage extends HyParViewMessage
{
    public enum Result { ACCEPT, DENY }

    public final Result result;
    public final int neighborRequestsCount;

    public NeighborResponseMessage(InetAddress requestor, String datacenter, Result result, int neighborRequestsCount)
    {
        super(requestor, datacenter);
        this.result = result;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_RESPONSE;
    }
}
