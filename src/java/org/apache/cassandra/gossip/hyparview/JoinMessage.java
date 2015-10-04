package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public class JoinMessage extends HyParViewMessage
{
    public JoinMessage(HPVMessageId messgeId, InetAddress sender, String datacenter)
    {
        super(messgeId, sender, datacenter, Optional.<HPVMessageId>empty());
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }
}
