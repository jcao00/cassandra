package org.apache.cassandra.gms2.gossip;

/**
 * Interface for systems responsible for sending messages in an epidemic manner (a/k/a gossip).
 */
public interface GossipBroadcaster
{
    void broadcast(String clientId, Object messageId, Object message);
}
