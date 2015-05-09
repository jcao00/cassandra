package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynMessage extends AntiEntropyMessage
{
    private final Object data;

    public SynMessage(String clientId, Object data)
    {
        super(clientId);
        this.data = data;
    }

    public Object getData()
    {
        return data;
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
