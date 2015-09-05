package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class JoinResponseMessage extends HyParViewMessage
{
    public JoinResponseMessage(InetAddress requestor, String datacenter)
    {
        super(requestor, datacenter);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN_RESPONSE;
    }
}
