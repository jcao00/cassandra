package org.apache.cassandra.gms2.gossip.peersampling.messages;

import org.apache.cassandra.io.ISerializer;

public interface HyParViewMessage
{
    MessageType getMessageType();

    ISerializer getSerializer();
}
