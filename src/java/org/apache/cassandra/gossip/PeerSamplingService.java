package org.apache.cassandra.gossip;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * A Peer Sampling Service
 * TODO: author name and link to paper
 *
 * provides a restricted view of a cluster to dependent components, thus allowing them
 * to be more efficient with connections and other resources (not needing to connect to
 * all peers in the cluster).
 */
public interface PeerSamplingService
{
    /**
     * Allow the component to initialize. Should be called before {@code register}'ing any listeners
     * or allowing listeners to call {@code getPeers}.
     *
     * @param messageSender Service that sends messages to peer nodes.
     * @param executorService An ExecutorService to which internal messages can be sent (instead of handling events inline from
     *                        whatever thread broadcast them).
     */
    void init(MessageSender messageSender, ExecutorService executorService);

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