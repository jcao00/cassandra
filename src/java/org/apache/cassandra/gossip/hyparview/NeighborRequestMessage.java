package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public class NeighborRequestMessage extends HyParViewMessage
{
    public enum Priority { HIGH, LOW }

    public final Priority priority;

    /**
     * A simple counter for the number of times the node has sent neighbor requests. We use this so we can stop sending
     * requests after some number of rejections.
     */
    public final int neighborRequestsCount;

    public NeighborRequestMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, Priority priority,
                                  int neighborRequestsCount, Optional<HPVMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
        this.priority = priority;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_REQUEST;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(200);
        sb.append(super.toString());
        sb.append(", priority ").append(priority);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof NeighborRequestMessage))
            return false;
        NeighborRequestMessage msg = (NeighborRequestMessage)o;

        return priority.equals(msg.priority) && neighborRequestsCount == msg.neighborRequestsCount;
    }
}
