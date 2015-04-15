package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;

/**
 */
public interface GossipBroadcaster
{
    void broadcast(String clientId, Object messageId, Object message);

    /**
     * A callback from the {@link PeerSamplingService} indicating
     * this broadcaster has been registered.
     */
    void registered(PeerSamplingService peerSamplingService);

    /**
     * Callback from the {@link PeerSamplingService}
     * @param peer A peer that is now up.
     */
    void neighborUp(InetAddress peer);

    /**
     * Callback from the {@link PeerSamplingService}
     * @param peer A peer that is now down.
     */
    void neighborDown(InetAddress peer);
}
