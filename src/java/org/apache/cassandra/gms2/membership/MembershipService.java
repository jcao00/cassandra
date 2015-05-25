package org.apache.cassandra.gms2.membership;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Objects;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyClient;
import org.apache.cassandra.gms2.membership.messages.ClusterMembershipMessage;
import org.apache.cassandra.gms2.membership.messages.PeerStateMessage;

import static org.apache.cassandra.gms2.membership.messages.ClusterMembershipMessage.Action.ADD;

/**
 * A cluster membership service for cassandra.
 *
 * The membership service communicates with peers with two types of messages: one for cluster membership, and another
 * for the state of the peers in the cluster. The cluster membership messages are part of a CRDT representing what nodes
 * are in the cluster, while the peer state messages pass any changes in a node's state (status, DC, rack, load, and so on).
 */
public class MembershipService implements BroadcastClient, AntiEntropyClient
{
    private static final String ID = "membership_svc";
    private static final String MSG_ID_PREFIX_CLUSTER_MEMBERSHIP = "CM_";
    private static final String MSG_ID_PREFIX_PEER_STATE = "PS_";

    private final GossipBroadcaster broadcaster;
    private final MembershipConfig config;

    /**
     * A CRDT of the nodes that are in the cluster.
     */
    private final Orswot<MemberEntry, InetAddress> members;

    /**
     * A map to hold the known state of each of the nodes in the cluster.
     */
    private final ConcurrentMap<InetAddress, PeerState> peerStateMap;

    private final List<IEndpointStateChangeSubscriber> lifecycleSubscribers;

    public MembershipService(GossipBroadcaster broadcaster, MembershipConfig config)
    {
        this.broadcaster = broadcaster;
        this.config = config;
        members = new Orswot(config.getAddress());
        lifecycleSubscribers = new ArrayList<>(1);
        peerStateMap = new ConcurrentHashMap<>();
    }

    public void init(ScheduledExecutorService scheduledService)
    {
        // TODO: is there anything that *really* needs to scheduled??

        // TODO: send one-way msg to seed to Add ... and block for response!!!
        broadcaster.
    }

    /**
     * Process an event directly from a peer that wants to join/rejoin/leave/be kicked out of the cluster.
     * Typically, these messages are processed by a seed node, but the receiving node does not need to think itself
     * is a seed node.
     */
    public void handle(ClusterMembershipMessage message, InetAddress sender)
    {
        switch (message.getAction())
        {
            case ADD:       handleAdd(message, sender); break;
            case REMOVE:    handleRemove(message, sender); break;
            default:
                throw new IllegalArgumentException("unknown action type: " + message.getAction());
        }
    }

    void handleAdd(ClusterMembershipMessage message, InetAddress sender)
    {
        OrswotClock<InetAddress> clock = members.add(new MemberEntry(message.getAddress()));
        // TODO: add metatdata to map
        // TODO: broadcast add msg

       // ClusterMembershipMessage msg = new ClusterMembershipMessage();
      //  broadcaster.broadcast(ID, msg.generateId(), msg);

        // TODO: send response back to sender (with full clock and metadata)
        // need a one-way function
    }

    void handleRemove(ClusterMembershipMessage message, InetAddress sender)
    {
        OrswotClock<InetAddress> removeClock = members.remove(new MemberEntry(message.getAddress()));
        // TODO: broadcast remove msg

        // TODO: send response back to sender ??
    }

    /*
        methods for BroadcastClient
    */

    public boolean receiveBroadcast(Object messageId, Object message) throws IOException
    {
        // TODO: replace this string checking horse shit, and add some 'Type' to the message
        String msgId = messageId.toString();
        if (msgId.startsWith(MSG_ID_PREFIX_CLUSTER_MEMBERSHIP))
            return handleClusterMembershipMessage(msgId, (ClusterMembershipMessage)message);
        else
            return handlePeerStateMessage(msgId, (PeerStateMessage)message);
    }

    boolean handleClusterMembershipMessage(String msgId, ClusterMembershipMessage message)
    {
        switch (message.getAction())
        {
            case ADD:       return handleBroadcastAdd(message);
            case REMOVE:    return handleBroadcastRemove(message);
            default:
                throw new IllegalArgumentException("unknown action type: " + message.getAction());
        }
    }

    private boolean handleBroadcastAdd(ClusterMembershipMessage message)
    {
        if (!members.applyAdd(new MemberEntry(message.getAddress()), message.getClock()))
           return false;
        //TODO: add metatdata to map
        return true;
    }

    private boolean handleBroadcastRemove(ClusterMembershipMessage message)
    {
        if (!members.applyRemove(new MemberEntry(message.getAddress()), message.getClock()))
            return false;
        // TODO: remove metatdata - might be a simple map.remove(), but not sure
        return true;
    }

    boolean handlePeerStateMessage(String msgId, PeerStateMessage peerStateMessage)
    {
        // msgId format = prefix + addr + generation + highest version
        PeerState peerState = peerStateMap.get(peerStateMessage.getAddress());
        if (peerState == null)
        {
            peerState = new PeerState(peerStateMessage.getGeneration(), peerStateMessage.getAppStates());
            PeerState existing = peerStateMap.putIfAbsent(peerStateMessage.getAddress(), peerState);
            if (existing == null)
                return true;

            return mergePeerState(existing, peerStateMessage);
        }
        else
        {
            return mergePeerState(peerState, peerStateMessage);
        }
    }

    private boolean mergePeerState(PeerState peerState, PeerStateMessage peerStateMessage)
    {
        // we have a newer generation, thus we must have more recent information
        if (peerState.generation > peerStateMessage.getGeneration())
            return false;

        // we have an older generation, thus we are out of date
        if (peerState.generation < peerStateMessage.getGeneration())
        {
            // TODO: introduce atomic reference around the PeerState in the map, to be really thread safe?
            peerStateMap.put(peerStateMessage.getAddress(), new PeerState(peerStateMessage.getGeneration(), peerStateMessage.getAppStates()));
            // TODO: invoke subscribers?
            return true;
        }

        // if we got here, the generations are the same, so we have to merge state by state
        boolean newData = true;
        for (Map.Entry<ApplicationState, Integer> state : peerStateMessage.getAppStates().entrySet())
        {
            Integer version = peerState.applicationStates.get(state.getKey());
            if (version == null || version.intValue() < state.getValue())
            {
                peerState.applicationStates.put(state.getKey(), version);
                newData &= true;
                // TODO: invoke subscribers?
            }
            else
            {
                newData &= false;
            }
        }

        return newData;
    }

    public boolean hasReceivedBroadcast(Object messageId)
    {
        // TODO : implement me
        return false;
    }

    public String getClientId()
    {
        return ID;
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

    static class MemberEntry
    {
        final InetAddress address;

        MemberEntry(InetAddress address)
        {
            this.address = address;
        }

        public boolean equals(Object o)
        {
            if (o == null || !(o instanceof MemberEntry))
                return false;
            if (o == this)
                return true;
            MemberEntry entry = (MemberEntry)o;
            return address.equals(entry);
        }

        public int hashCode()
        {
            return Objects.hashCode(address);
        }

        public String toString()
        {
            return address.toString();
        }
    }
}
