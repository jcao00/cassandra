package org.apache.cassandra.gossip;

public interface BroadcastServiceClient
{
    /**
     * A unique name for this client.
     */
    String getClientName();

    /**
     * Handle a broadcasted message.
     *
     * @return true if the has not been seen before (not delivered earlier);
     * else, false if it is a duplicate.
     */
    boolean receive(Object payload);
}
