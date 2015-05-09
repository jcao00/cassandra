package org.apache.cassandra.gms2.membership;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyClient;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

/**
 * A cluster membership service for cassandra.
 */
public class MembershipService implements BroadcastClient, AntiEntropyClient
{
    private static final String ID = "membership_svc";

    private final Orswot<? extends Object> members;

    private final Map<InetSocketAddress, PeerState> peerStateMap;
    private final List<IEndpointStateChangeSubscriber> lifecycleSubscribers;

    private GossipBroadcaster broadcaster;

    public MembershipService(InetAddress localAddr)
    {
        members = new Orswot(localAddr);
        lifecycleSubscribers = new ArrayList<>(4);
        peerStateMap = new ConcurrentHashMap<>();
    }

    //TODO: when size of membership changes, callback to the Broadcast service (HPV)
    // so it can adjust it's active/passive view size, ARWL/PRLW, and so on

    public Collection<InetAddress> getPeers()
    {
        // TODO: return some immutable list of peers
        return null;
    }

    public String getClientId()
    {
        return ID;
    }

    public boolean receiveBroadcast(Object messageId, Object message) throws IOException
    {
        return true;
    }

    public boolean hasReceivedMessage(Object messageId)
    {
        return false;
    }

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void register(IEndpointStateChangeSubscriber subscriber)
    {
        lifecycleSubscribers.add(subscriber);
    }

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    public void unregister(IEndpointStateChangeSubscriber subscriber)
    {
        lifecycleSubscribers.remove(subscriber);
    }

    /*
        methods for AntiEntropyClient
     */

    public Object preparePush()
    {
        return null;
    }

    public Object processPush(Object t) throws IOException
    {
        return null;
    }

    public Object processPull(Object t) throws IOException
    {
        return null;
    }

    public void processPushPull(Object t) throws IOException
    {

    }
}
