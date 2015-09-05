package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class NeighborResponseMessage extends HyParViewMessage
{
    public enum Result { ACCEPT, DENY }

    public final Result result;



    public NeighborResponseMessage(InetAddress requestor, String datacenter, Result result)
    {
        super(requestor, datacenter);
        this.result = result;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_RESPONSE;
    }
}
