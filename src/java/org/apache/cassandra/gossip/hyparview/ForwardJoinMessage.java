package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class ForwardJoinMessage extends HyParViewMessage
{
    public final InetAddress forwarder;
    public final int timeToLive;

    public ForwardJoinMessage(InetAddress requestor, String datacenter, InetAddress forwarder, int timeToLive)
    {
        super(requestor, datacenter);
        this.forwarder = forwarder;
        this.timeToLive = timeToLive;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.FORWARD_JOIN;
    }
}
