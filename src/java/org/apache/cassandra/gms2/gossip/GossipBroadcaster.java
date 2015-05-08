package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;

/**
 */
public interface GossipBroadcaster
{
    void broadcast(String clientId, Object messageId, Object message);
}
