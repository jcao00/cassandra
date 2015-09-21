package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Collections;

public class DisconnectMessage extends HyParViewMessage
{
    public DisconnectMessage(HPVMessageId messgeId, InetAddress peer, String datacenter)
    {
        super(messgeId, peer, datacenter, Collections.emptyMap());
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }
}
