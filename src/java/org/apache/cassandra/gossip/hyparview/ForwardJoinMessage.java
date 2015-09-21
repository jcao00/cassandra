package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Map;

public class ForwardJoinMessage extends HyParViewMessage
{
    /**
     * Address of the peer that is trying to join.
     */
    public final InetAddress originator;

    /**
     * The datacenter of the originator.
     */
    public final String originatorDatacenter;

    public final int timeToLive;

    public ForwardJoinMessage(HPVMessageId messgeId, InetAddress sender, String senderDatacenter, InetAddress originator,
                              String originatorDatacenter, int timeToLive, Map<InetAddress, HPVMessageId> lastDisconnect)
    {
        super(messgeId, sender, senderDatacenter, lastDisconnect);
        this.originator = originator;
        this.originatorDatacenter = originatorDatacenter;
        this.timeToLive = timeToLive;
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
}
