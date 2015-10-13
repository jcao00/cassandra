package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;

public class JoinMessage extends HyParViewMessage
{
    public JoinMessage(GossipMessageId messgeId, InetAddress sender, String datacenter)
    {
        super(messgeId, sender, datacenter, Optional.<GossipMessageId>empty());
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }
}
