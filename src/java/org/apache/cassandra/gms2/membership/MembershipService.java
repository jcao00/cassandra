package org.apache.cassandra.gms2.membership;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyClient;

/**
 * A cluster membership service for cassandra.
 */
public class MembershipService implements BroadcastClient, AntiEntropyClient
{
    private static final String ID = "membership_svc";

    private final GossipBroadcaster broadcaster;
    private final MembershipConfig config;

    /**
     * A CRDT of the nodes that are in the cluster.
     */
    private final Orswot<? extends Object> members;

    /**
     * A map to hold the known state of each of the nodes in the cluster.
     */
    private final Map<InetAddress, PeerState> peerStateMap;

    private final List<IEndpointStateChangeSubscriber> lifecycleSubscribers;

    public MembershipService(GossipBroadcaster broadcaster, MembershipConfig config)
    {
        this.broadcaster = broadcaster;
        this.config = config;
        members = new Orswot(config.getAddress());
        lifecycleSubscribers = new ArrayList<>(1);
        peerStateMap = new ConcurrentHashMap<>();
    }

    public String getClientId()
    {
        return ID;
    }

    public boolean receiveBroadcast(Object messageId, Object message) throws IOException
    {
        return true;
    }

    public boolean hasReceivedBroadcast(Object messageId)
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
