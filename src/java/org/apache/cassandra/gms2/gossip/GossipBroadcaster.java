package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;


/**
 * Together with {@link org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService}, form an implementation of the
 * <a html="http://lpdwww.epfl.ch/upload/documents/publications/neg--1184036295all.pdf">peer sampling service</a>.
 */
public interface GossipBroadcaster
{
    /**
     * A callback from the {@link org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService} indicating the this broadcaster has been registered.
     */
    void registered(PeerSamplingService peerSamplingService);

    void neighborUp(InetAddress peer);

    void neighborDown(InetAddress peer);
}
