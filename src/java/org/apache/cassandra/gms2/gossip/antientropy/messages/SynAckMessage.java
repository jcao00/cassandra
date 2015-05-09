package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynAckMessage extends AntiEntropyMessage
{
    private final Object data;

    public SynAckMessage(String clientId, Object data)
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
        return MessageType.SYN_ACK;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
