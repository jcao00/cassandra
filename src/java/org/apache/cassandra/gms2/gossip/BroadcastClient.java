package org.apache.cassandra.gms2.gossip;

import java.io.IOException;

/**
 * A client to participate in receiving broadcasted messages. The contract for a client is two-fold:
 * <ul>
 *     <li>receive and process any broadcast incoming messages (obviously)</li>
 *     <li>periodically produce a summary of state to exchange with peers for anti-entropy purposes.
 *     The summary is free for the implementation to decide, but examples may include: a merkle tree of the entire client's
 *     state, a logical clock of the current state, a list of message IDs received since the last summary invocation, and so on.</li>
 * </ul>
 */
public interface BroadcastClient
{
    /**
     * @return globally unique identifier for this client system. will be transmitted in each message.
     */
    String getClientId();

    /**
     * @return true if the message has not been previously received; else, false, if it was stale
     */
    boolean receiveBroadcast(Object messageId, Object message) throws IOException;

    Object prepareSummary();

    void receiveSummary(Object summary);
}

