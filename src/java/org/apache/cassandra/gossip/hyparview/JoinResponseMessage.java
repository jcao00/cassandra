package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class JoinResponseMessage extends HyParViewMessage
{
    public JoinResponseMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, HPVMessageId lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN_RESPONSE;
    }
}
