package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynMessage extends AntiEntropyMessage
{
    private final Object pushData;

    public SynMessage(String clientId, Object pushData)
    {
        super(clientId);
        this.pushData = pushData;
    }

    public Object getPushData()
    {
        return pushData;
    }

    public MessageType getMessageType()
    {
        return MessageType.SYN;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
