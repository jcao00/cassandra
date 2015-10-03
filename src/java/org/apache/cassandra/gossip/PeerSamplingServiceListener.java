package org.apache.cassandra.gossip;

import java.net.InetAddress;

/**
 * Callback interface for consumers of the {@code PeerSamplingService}.
 */
public interface PeerSamplingServiceListener
{
    /**
     * Triggered when a node is added to the Peer Sampling Service's active view.
     *
     * @param peer The new peer that was added to the view.
     * @param datacenter The datacenter the new peer is located in.
     */
    void neighborUp(InetAddress peer, String datacenter);

    /**
     * Triggered when a node is removed from the Peer Sampling Service's active view.
     *
     * @param peer The peer that was removed the view.
     * @param datacenter The datacenter the peer is located in.
     */
    void neighborDown(InetAddress peer, String datacenter);
}
