package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;

public class DisconnectMessage extends HyParViewMessage
{
    public DisconnectMessage(GossipMessageId messgeId, InetAddress peer, String datacenter)
    {
        super(messgeId, peer, datacenter, Optional.<GossipMessageId>empty());
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }
}
