package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.Future;

import org.apache.cassandra.net.IAsyncCallback;

/**
 * Interface for systems responsible for broadcasting messages in an epidemic manner (a/k/a gossip).
 */
public interface GossipBroadcaster
{
    void broadcast(String clientId, Object messageId, Object message);

    Future r
}
