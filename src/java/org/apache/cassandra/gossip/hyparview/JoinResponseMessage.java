package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public class JoinResponseMessage extends HyParViewMessage
{
    public JoinResponseMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, Optional<HPVMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN_RESPONSE;
    }
}
