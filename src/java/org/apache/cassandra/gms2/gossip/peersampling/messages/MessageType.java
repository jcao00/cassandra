package org.apache.cassandra.gms2.gossip.peersampling.messages;

public enum MessageType
{
    // ordinal order is used externally; do not reorder
    JOIN,
    FORWARD_JOIN,
    JOIN_ACK,
    DISCONNECT,
    NEIGHBOR_REQUEST,
    NEIGHBOR_RESPONSE,
    SHUFFLE,
    SHUFFLE_RESPONSE
}
