package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;

public class ForwardJoinMessage extends HyParViewMessage
{
    /**
     * Address of the peer that is trying to join.
     */
    private final InetAddress originator;

    /**
     * The datacenter of the originator.
     */
    private final String originatorDatacenter;

    private final GossipMessageId originatorMessageId;

    /**
     * The number of steps remaining to forward the message. Not a TTL as in seconds.
     */
    public final int timeToLive;

    public ForwardJoinMessage(GossipMessageId messgeId, InetAddress sender, String senderDatacenter, InetAddress originator,
                              String originatorDatacenter, int timeToLive, GossipMessageId originatorId)
    {
        super(messgeId, sender, senderDatacenter, Optional.<GossipMessageId>empty());
        this.originator = originator;
        this.originatorDatacenter = originatorDatacenter;
        this.timeToLive = timeToLive;
        this.originatorMessageId = originatorId;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.FORWARD_JOIN;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(200);
        sb.append(super.toString());
        sb.append(", originator ").append(originator).append(" (").append(originatorDatacenter).append(") ");
        return sb.toString();
    }

    public InetAddress getOriginator()
    {
        return originator;
    }

    public String getOriginatorDatacenter()
    {
        return originatorDatacenter;
    }

    public GossipMessageId getOriginatorMessageId()
    {
        return originatorMessageId;
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof ForwardJoinMessage))
            return false;
        ForwardJoinMessage msg = (ForwardJoinMessage)o;

        // all originator* fields should be checked in super.equals(), so just the custom fields here
        return timeToLive == msg.timeToLive;
    }
}
