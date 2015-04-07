package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public interface AntiEntropyMessage
{
    MessageType getMessageType();

    ISerializer getSerializer();
}
