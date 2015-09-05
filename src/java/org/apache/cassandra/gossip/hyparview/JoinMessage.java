package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class JoinMessage extends HyParViewMessage
{
    public JoinMessage(InetAddress requestor, String datacenter)
    {
        super(requestor, datacenter);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }
}
