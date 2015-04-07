package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynMessage implements AntiEntropyMessage
{
    public MessageType getMessageType()
    {
        return MessageType.SYN;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
