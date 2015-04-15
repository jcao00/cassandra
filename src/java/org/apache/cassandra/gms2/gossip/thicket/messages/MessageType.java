package org.apache.cassandra.gms2.gossip.thicket.messages;

public enum MessageType
{
    DATA,
    PRUNE,
    SUMMARY,
    GRAFT_REQUEST,
    GRAFT_RESPONSE,
}
