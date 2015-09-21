package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Map;

public class JoinMessage extends HyParViewMessage
{
    public JoinMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, Map<InetAddress, HPVMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }
}
