package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gms2.gossip.GossipBroadcaster;

/**
 * Together with {@link org.apache.cassandra.gms2.gossip.GossipBroadcaster}, form an implementation of the
 * <a html="http://lpdwww.epfl.ch/upload/documents/publications/neg--1184036295all.pdf">peer sampling service</a>.
 */
public interface PeerSamplingService
{
    void register(PeerSamplingServiceClient client);

    Collection<InetAddress> getPeers();
}
