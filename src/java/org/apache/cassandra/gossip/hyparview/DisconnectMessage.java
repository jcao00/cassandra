package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class DisconnectMessage extends HyParViewMessage
{
    public DisconnectMessage(InetAddress peer, String datacenter)
    {
        super(peer, datacenter);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }
}
