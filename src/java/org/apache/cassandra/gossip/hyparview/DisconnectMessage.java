package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public class DisconnectMessage extends HyParViewMessage
{
    public DisconnectMessage(HPVMessageId messgeId, InetAddress peer, String datacenter)
    {
        super(messgeId, peer, datacenter, Optional.<HPVMessageId>empty());
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }
}
