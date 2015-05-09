package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class AckMessage extends AntiEntropyMessage
{
    private final Object data;

    public AckMessage(String clientId, Object pullData)
    {
        super(clientId);
        this.data = pullData;
    }

    public Object getData()
    {
        return data;
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
