package org.apache.cassandra.gms2.gossip.antientropy;

import java.io.IOException;

import org.apache.cassandra.io.ISerializer;

/**
 * Interface to participate in the periodic, peer-to-peer, batch-style anti-entropy.
 * Clients may choose any protocol or format for the exchange of data, such as digests, logical clocks, blobs, and so on.
 * The only imposed constraint is the messaging workflow. The basic workflow for two nodes, A and B, looks like this:
 *
 * 1. Node A calls {@code preparePush} to get initial data to send to B
 *
 * 2. Node B receives message (SYN), calls @{code processPush}, and returns it's state which will be sent to A.
 *
 * 3. Node A receives message (ACK), calls @{code processPull}, and has the option of returning data. If no data is returned,
 * the anti-entropy session is considered complete. If data is returned, it will be sent to B.
 *
 * 4. Node B receives message (SYN_ACK), and calls {@code processPushPull}. The anti-entropy is complete.
 */
public interface AntiEntropyClient
{
    String getClientId();

    Object preparePush();

    Object processPush(Object t) throws IOException;

    Object processPull(Object t) throws IOException;

    void processPushPull(Object t) throws IOException;
}
