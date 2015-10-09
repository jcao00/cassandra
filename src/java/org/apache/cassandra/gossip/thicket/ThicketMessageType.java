package org.apache.cassandra.gossip.thicket;

public enum ThicketMessageType
{
    DATA,
    SUMMARY,
    PRUNE,
    GRAFT,
}
