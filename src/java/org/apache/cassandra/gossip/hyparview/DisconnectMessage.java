package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class DisconnectMessage extends HyParViewMessage
{
    public DisconnectMessage(HPVMessageId messgeId, InetAddress peer, String datacenter)
    {
        super(messgeId, peer, datacenter, null);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }
}
