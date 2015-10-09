package org.apache.cassandra.gossip;

public interface BroadcastService
{
    void broadcast(Object messageId, Object payload);

    void register(BroadcastServiceClient client);
}
