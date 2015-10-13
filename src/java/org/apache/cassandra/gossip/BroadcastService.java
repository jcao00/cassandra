package org.apache.cassandra.gossip;

public interface BroadcastService
{
    void broadcast(Object payload, BroadcastServiceClient client);

    void register(BroadcastServiceClient client);
}
