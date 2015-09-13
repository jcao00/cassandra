package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class NeighborRequestMessage extends HyParViewMessage
{
    public enum Priority { HIGH, LOW }

    public final Priority priority;

    /**
     * A simple counter for the number of times the node has sent neighbor requests. We use this so we can stop sending
     * requests after some number of rejections.
     */
    public final int neighborRequestsCount;

    public NeighborRequestMessage(InetAddress requestor, String datacenter, Priority priority, int neighborRequestsCount)
    {
        super(requestor, datacenter);
        this.priority = priority;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_REQUEST;
    }
}
