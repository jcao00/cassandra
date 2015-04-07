package org.apache.cassandra.gms2.membership;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyClient;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyService;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

/**
 * A cluster membership service for cassandra.
 *
 * While not strictly a component of the gossip susbsystem, membership is a special component,
 * if not only due to historical reasons, but also the anti-entropy part of gossip prefers
 * peers not already being used by the broadcast/peer sampling services.
 */
public class MembershipService implements PeerSamplingService, AntiEntropyClient
{
    // TODO: in the future, this will be the ORSWOT CRDT, but a placeholder for now
    private final Orswot<? extends Object> members;

    private final Map<InetSocketAddress, PeerState> peerStateMap;
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers;

    public MembershipService(InetAddress localAddr)
    {
        members = new Orswot(localAddr);
        lifecycleSubscribers = new ArrayList<>(4);
        peerStateMap = new ConcurrentHashMap<>();
    }

    //TODO: when size of membership changes, callback to the Broadcast service (HPV)
    // so it can adjust it's active/passive view size, ARWL/PRLW, and so on

    public void init(AntiEntropyService antiEntropy)
    {
        antiEntropy.register(this);
    }

    public void register(GossipBroadcaster broadcaster)
    {
        // nop, for now
    }

    public Collection<InetAddress> getPeers()
    {
        // TODO: return some immutable list of peers
        return null;
    }

    public void receive(ISerializer iSerializer) throws IOException
    {

    }

    public ISerializer getDataToSend()
    {
        return null;
    }
}
