package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class AckMessage implements AntiEntropyMessage
{
    public MessageType getMessageType()
    {
        return MessageType.ACK;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
