package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gossip.hyparview.HyParViewMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Simple interface to abstract away message sending.
 * Exists so we don't depend directly on MessagingService (so we can test without it).
 */
public interface MessageSender
{
    /**
     * Send a message to a peer node.
     *
     * @param destinationAddr Where to send the message
     * @param message The payload
     */
    void send(InetAddress destinationAddr, HyParViewMessage message);
}
