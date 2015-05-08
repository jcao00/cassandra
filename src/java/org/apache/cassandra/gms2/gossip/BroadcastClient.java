package org.apache.cassandra.gms2.gossip;

import java.io.IOException;
import java.util.Set;

/**
 * A client to participate in receiving broadcasted messages.
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

    Object prepareExchange();

    /**
     *
     * @param summary
     * @return Set of missing messageIds
     */
    Set<? extends Object> receiveSummary(Object summary);

    boolean hasReceivedMessage(Object messageId);
}

