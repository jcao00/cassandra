package org.apache.cassandra.gossip;

import org.apache.cassandra.io.IVersionedSerializer;

public interface BroadcastServiceClient<T>
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
    boolean receive(T payload);

    IVersionedSerializer<T> getSerializer();
}
