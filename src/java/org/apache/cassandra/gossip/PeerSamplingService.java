package org.apache.cassandra.gossip;

import java.net.InetAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A Peer Sampling Service is an implementation of
 * <a href="http://infoscience.epfl.ch/record/83409/files/neg--1184036295all.pdf">
 *     The Peer Sampling Service: Experimental Evaluation of Unstructured Gossip-Based Implementations</a>.
 * In brief, a peer sampling service provides a restricted view of a cluster to dependent components, thus allowing them
 * to be more efficient with connections and other resources (by not needing to connect to all peers in the cluster).
 */
public interface PeerSamplingService
{
    /**
     * Allow the component to initialize. Should be called before {@code register}'ing any listeners
     * or allowing listeners to call {@code getPeers}.
     *
     * @param epoch The generation of the current {@link GossipContext}
     */
    void start(int epoch);

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

    void shutdown();
}