package org.apache.cassandra.gms2.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;

import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;

public class MinimalPeerSamplingService implements PeerSamplingService
{
    private final Collection<InetAddress> peers;

    public MinimalPeerSamplingService(Collection<InetAddress> peers)
    {
        this.peers = peers;
    }

    public void register(GossipBroadcaster broadcaster)
    {

    }

    public Collection<InetAddress> getPeers()
    {
        return peers;
    }

    public void addPeer(InetAddress peer)
    {
        peers.add(peer);
    }

    public boolean removePeer(InetAddress peer)
    {
        return peers.remove(peer);
    }
}
