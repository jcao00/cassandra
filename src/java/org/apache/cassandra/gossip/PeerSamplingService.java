package org.apache.cassandra.gossip;

import java.net.InetAddress;
import java.util.Collection;

/**
 * A Peer Sampling Service
 * TODO: author name and link to paper
 *
 * provides a limited view of a cluster to dependent components, thus allowing them
 * to be more efficient with connections and other resources (not needing to connect to
 * all peers in the cluster).
 */
public interface PeerSamplingService
{
    /**
     * Allow the component to initialize. Should be called before {@code register}'ing any listeners
     * or allowing listeners to call {@code getPeers}.
     */
    void init(MessageSender messageSender);

    /**
     * Retrieve all the peers in the active view.
     */
    Collection<InetAddress> getPeers();

    /**
     * Register a listener for callbacks from the peer sampling service.
     */
    void register(PeerSamplingServiceListener listener);

    /**
     * Unregister a listener from the peer sampling service.
     */
    void unregister(PeerSamplingServiceListener listener);
}