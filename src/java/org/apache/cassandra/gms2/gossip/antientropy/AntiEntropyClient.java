package org.apache.cassandra.gms2.gossip.antientropy;

import java.io.IOException;

import org.apache.cassandra.io.ISerializer;

/**
 * Interface to participate in the periodic, batch-style anti-entropy.
 *
 * @param <T>
 */
public interface AntiEntropyClient<T extends ISerializer<T>>
{
    T getDataToSend();

    void receive(T t) throws IOException;
}
