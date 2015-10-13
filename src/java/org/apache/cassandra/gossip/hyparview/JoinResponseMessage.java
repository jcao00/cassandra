package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;

public class JoinResponseMessage extends HyParViewMessage
{
    public JoinResponseMessage(GossipMessageId messgeId, InetAddress sender, String datacenter, Optional<GossipMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN_RESPONSE;
    }
}
