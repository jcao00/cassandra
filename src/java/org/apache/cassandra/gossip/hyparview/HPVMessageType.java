package org.apache.cassandra.gossip.hyparview;

public enum HPVMessageType
{
    JOIN,
    JOIN_RESPONSE,
    FORWARD_JOIN,
    NEIGHBOR_REQUEST,
    NEIGHBOR_RESPONSE,
    DISCONNECT,
}
