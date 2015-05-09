package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class AckMessage extends AntiEntropyMessage
{
    private final Object pullData;

    public AckMessage(String clientId, Object pullData)
    {
        super(clientId);
        this.pullData = pullData;
    }

    public Object getPullData()
    {
        return pullData;
    }

    public MessageType getMessageType()
    {
        return MessageType.ACK;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
