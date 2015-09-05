package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class NeighborRequestMessage extends HyParViewMessage
{
    public enum Priority { HIGH, LOW }

    public final Priority priority;

    public NeighborRequestMessage(InetAddress requestor, String datacenter, Priority priority)
    {
        super(requestor, datacenter);
        this.priority = priority;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_REQUEST;
    }
}
