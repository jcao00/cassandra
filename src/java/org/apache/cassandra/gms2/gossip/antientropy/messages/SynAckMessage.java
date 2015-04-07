package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynAckMessage implements AntiEntropyMessage
{
    public MessageType getMessageType()
    {
        return MessageType.SYN_ACK;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
