package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public class JoinMessage extends HyParViewMessage
{
    public JoinMessage(HPVMessageId messgeId, InetAddress sender, String datacenter)
    {
        super(messgeId, sender, datacenter, null);
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }
}
