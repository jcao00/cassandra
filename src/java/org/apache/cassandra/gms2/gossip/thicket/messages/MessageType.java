package org.apache.cassandra.gms2.gossip.thicket.messages;

public enum MessageType
{
    PRUNE,
    GRAFT,
    IHAVE,
    IHAVE_ACK,
}
