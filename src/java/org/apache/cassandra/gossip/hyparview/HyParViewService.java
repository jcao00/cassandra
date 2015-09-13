package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;
import org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority;
import org.apache.cassandra.gossip.hyparview.NeighborResponseMessage.Result;
import org.apache.cassandra.locator.SeedProvider;

/**
 * An implementation of the HyParView paper, Leitao, et al, 2008
 * http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf. Briefly, HyParView is a {@code PeerSamplingService}
 * that maintains a view of subset of the peers in cluster that are to be used by dependent modules
 * (for example, upper-layer goosip broadcast components). HyParView uses a loose coordination protocol
 * to ensure all nodes are included in at least one peer's view (ensuring complete coverage). Also, each node
 * maintains a symmetric relationship with each peer in it's active view: if node A had node B in it's active view,
 * B should have A in it's own active view.
 *
 * However, we take some deviations with our implementation. HyParView requires two views of peers
 * in the cluster: an active view and a passive view. We maintain the active in this class,
 * but we can piggy-back off the existing knowledge of the full cluster as a placement
 * for all the mechanics of maintaining the passive view (see 4.4 of the paper, especially
 * the part on the SHUFFLE messages). We still fulfill the needs of the passive view (having a backlog
 * of peers to use as a backup), but save ourselves some code/network traffic/extra functionality.
 *
 * As for the active view, we split it into two parts: a view of active peers for the local datacenter,
 * and a map which will hold one active peer for each remote datacenter. This way, we can keep
 * at least one node from every datacenter in the "aggregate active view". Note: the HyParView paper
 * does not deal with network partitions (it was solving other problems), and so this is our way of handling
 * the need to keep all DCs in the active view, as much as we can.
 *
 * Due to the requirement for symmetric connections between peers (that is, if B is in A's view, A should be in B's view),
 * and the fact that datacenters can have different numbers of nodes, not all nodes will have connections to other datacenters.
 * For example, if DC1 has 100 nodes and DC2 has 10 nodes, DC2 cannot support symmetric connections to all of the nodes in DC1
 * (assuming each node has one connection for remote datacenters). However, because DC2 will have 10 connections to DC1,
 * and because DC1 will connected overlay within itself, we are safe in knowing the two datacenters are connected and that
 * events in DC2 will be propagated throughout DC1.
 *
 * Note: this class is *NOT* thread-safe, and intentionally so, in order to keep it simple and efficient.
 * With that restriction, though, is the requirement that mutations to state *must* happen on the
 * primary (gossip) thread.
 */
public class HyParViewService implements PeerSamplingService, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewService.class);
    private static final long DEFAULT_RANDOM_SEED = "BuyMyDatabass".hashCode();
    private static final int MAX_NEIGHBOR_REQUEST_ATTEMPTS = 2;

    private final Set<PeerSamplingServiceListener> listeners;

    @VisibleForTesting
    final EndpointStateSubscriber endpointStateSubscriber;

    /**
     * Active view of peers in the local datacenter. The max size of this collection
     * should hover around the max fanout value (based on number of nodes in the datacenter).
     */
    private final LinkedList<InetAddress> localDatacenterView;

    /**
     * Mapping of a datacenter name to a single peer in that datacenter.
     */
    private final Map<String, InetAddress> remoteView;

    /**
     * The braodcast address of the local node. We memoize it here to avoid a hard dependency on FBUtilities.
     */
    private final InetAddress localAddress;

    /**
     * Cassandra datacenter this node is executing in.
     */
    private final String datacenter;

    /**
     * Provider of cluster seeds. Memoized here to avoid a dependency on {@link org.apache.cassandra.config.DatabaseDescriptor}.
     */
    private final SeedProvider seedProvider;

    /**
     * Maximumu number of steps (or times) a FORWARD_JOIN request should be forwarded before being accepted and processed.
     */
    private final int activeRandomWalkLength;

    /**
     * A fixed local random number generator, mainly to provide consistency in testing.
     */
    private final Random random;

    @VisibleForTesting
    MessageSender messageSender;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduler;

    /**
     * Simple flag to indicate if we've received at least one response to a join request. Reset each time
     * {@code HyParViewService#join} is called.
     */
    private volatile boolean hasJoined;

    public HyParViewService(InetAddress localAddress, String datacenter, SeedProvider seedProvider, int activeRandomWalkLength)
    {
        this.localAddress = localAddress;
        this.datacenter = datacenter;
        this.seedProvider = seedProvider;
        this.activeRandomWalkLength = activeRandomWalkLength;
        random = new Random(DEFAULT_RANDOM_SEED);
        listeners = new HashSet<>();
        endpointStateSubscriber = new EndpointStateSubscriber();

        localDatacenterView = new LinkedList<>();
        remoteView = new HashMap<>();
    }

    public void init(MessageSender messageSender, ExecutorService executorService, ScheduledExecutorService scheduler)
    {
        this.messageSender = messageSender;
        this.executorService = executorService;
        this.scheduler = scheduler;

        // TODO:JEB fish out the initial Endpoint states
        // I will be thilled the day this dependency is severed!
        Gossiper.instance.register(endpointStateSubscriber);

        join();
        scheduler.scheduleWithFixedDelay(new ClusterConnectivityChecker(), 1, 1, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    void testInit(MessageSender messageSender, ExecutorService executorService, ScheduledExecutorService scheduler)
    {
        this.messageSender = messageSender;
        this.executorService = executorService;
        this.scheduler = scheduler;
    }

    /**
     * Sends out a message to a randomly selected seed node.
     */
    void join()
    {
        List<InetAddress> providedSeeds = seedProvider.getSeeds();
        if (providedSeeds == null)
        {
            logger.warn("no seeds defined - this should be fixed (will not start gossiping without seeds");
            return;
        }

        List<InetAddress> seeds = new ArrayList<>(providedSeeds);
        seeds.remove(localAddress);
        if (seeds.isEmpty())
        {
            logger.info("no seeds left in the seed list (after removing this node), so will wait for other nodes to join to start gossiping");
            return;
        }

        Collections.shuffle(seeds, random);
        messageSender.send(localAddress, seeds.get(0), new JoinMessage(localAddress, datacenter));
        scheduler.schedule(new JoinChecker(), 10, TimeUnit.SECONDS);
        hasJoined = false;
    }

    public void receiveMessage(HyParViewMessage message)
    {
        switch (message.getMessageType())
        {
            case JOIN: handleJoin((JoinMessage)message); break;
            case JOIN_RESPONSE: handleJoinResponse((JoinResponseMessage)message); break;
            case FORWARD_JOIN: handleForwardJoin((ForwardJoinMessage)message); break;
            case NEIGHBOR_REQUEST: handleNeighborRequest((NeighborRequestMessage)message); break;
            case NEIGHBOR_RESPONSE: handleNeighborResponse((NeighborResponseMessage)message); break;
            case DISCONNECT: handleDisconnect((DisconnectMessage)message); break;
            default:
                throw new IllegalArgumentException("Unhandled hyparview message type: " + message.getMessageType());
        }
    }

    /**
     * Handle an incoming request message to JOIN the HyParView subsystem. When we receive a join,
     * we add that node to our active view, possibly disconnecting from a node currently in the active view
     * (if we're at the max size limit). Then we send out a FORWARD_JOIN message to all peers in the active view.
     *
     * Note that if the requesting node is already in the active view, go ahead and reprocess the request
     * as it might be a re-broadcast from the sender (because it never got a response to it's JOIN request).
     */
    @VisibleForTesting
    void handleJoin(JoinMessage message)
    {
        addToView(message.requestor, message.datacenter);

        messageSender.send(localAddress, message.requestor, new JoinResponseMessage(message.requestor, message.datacenter));

        Collection<InetAddress> activeView = getPeers();
        activeView.remove(message.requestor);

        if (!activeView.isEmpty())
        {
            ForwardJoinMessage msg = new ForwardJoinMessage(message.requestor, message.datacenter, localAddress, activeRandomWalkLength);
            for (InetAddress activePeer : activeView)
                messageSender.send(localAddress, activePeer, msg);
        }
        else
        {
            logger.debug("no other nodes available to send a forward join message to");
        }
    }

    void addToView(InetAddress peer, String datacenter)
    {
        if (peer.equals(localAddress))
            return;

        // in case there's a (distributed) data race of when we get the membership update that the peer was added
        // versus when we get the request to add the peer.
        endpointStateSubscriber.add(peer, datacenter);

        boolean added;
        if (this.datacenter.equalsIgnoreCase(datacenter))
            added = addToLocalActiveView(peer);
        else
            added = addToRemoteActiveView(peer, datacenter);

        if (added)
            for (PeerSamplingServiceListener listener : listeners)
                listener.neighborUp(peer, datacenter);
    }

    /**
     * Add peer to the local active view, if it is not already in the view.
     *
     * @return true if the node was added to the active view; else, false.
     */
    private boolean addToLocalActiveView(InetAddress peer)
    {
        if (localDatacenterView.contains(peer))
            return false;

        localDatacenterView.addLast(peer);
        if (localDatacenterView.size() > endpointStateSubscriber.fanout(datacenter, datacenter))
            expungeNode(localDatacenterView.removeFirst(), datacenter);
        return true;
    }

    void expungeNode(InetAddress peer, String datacenter)
    {
        messageSender.send(localAddress, peer, new DisconnectMessage(peer, datacenter));

        for (PeerSamplingServiceListener listener : listeners)
            listener.neighborDown(peer, datacenter);
    }

    /**
     * @return return true if a new peer was set as the peer for the datacenter; else, false (if the same peer is the current value).
     */
    private boolean addToRemoteActiveView(InetAddress peer, String datacenter)
    {
        InetAddress existing = remoteView.get(datacenter);
        if (existing != null && existing.equals(peer))
            return false;

        remoteView.put(datacenter, peer);
        if (existing != null)
            expungeNode(existing, datacenter);
        return true;
    }

    /**
     * Handle a response to a join request. Mostly just need to create the symmetric connection (or entry)
     * in our active view, and disable any join checking mechanisms.
     */
    @VisibleForTesting
    void handleJoinResponse(JoinResponseMessage message)
    {
        hasJoined = true;
        addToView(message.requestor, message.datacenter);
    }

    /**
     * Handle an incoming forward join message. If the message's time-to-live is greater than 0,
     * forward the message to a node from the active view (avoiding sending back to the peer that forwarded
     * to us). If the message's time-to-live is 0, or we have <= 1 nodes in local DC's view (or remote datacenter view is empty),
     * add to local active view and respond back to requesting node.
     */
    @VisibleForTesting
    void handleForwardJoin(ForwardJoinMessage message)
    {
        int nextTTL = message.timeToLive - 1;
        boolean added = false;
        if (message.datacenter.equals(datacenter))
        {
            if (nextTTL <= 0 || localDatacenterView.size() <= 1)
            {
                addToView(message.requestor, datacenter);
                added = true;
            }
        }
        else
        {
            if (nextTTL <= 0 || !remoteView.containsKey(message.datacenter))
            {
                addToView(message.requestor, message.datacenter);
                added = true;
            }
        }

        if (added)
        {
            messageSender.send(localAddress, message.requestor, new JoinResponseMessage(localAddress, datacenter));
        }
        else
        {
            Optional<InetAddress> peer = findArbitraryTarget(message.requestor, message.datacenter);
            if (peer.isPresent())
                messageSender.send(localAddress, peer.get(), new ForwardJoinMessage(message.requestor, message.datacenter, localAddress, nextTTL));
        }
    }


    /**
     * Find a random target node, in the requested datacenter. If the datacenter is remote,
     * first try the peer that is in the active view for that datacenter. If there is no active peer,
     * try to select another node from that datacenter (assuming we want to keep the forwarding in that datacenter).
     * If there's no other known nodes in that datacenter, fall back to forwarding to a node in the current datacenter.
     */
    Optional<InetAddress> findArbitraryTarget(InetAddress filter, String datacenter)
    {
        List<InetAddress> candidates;
        if (this.datacenter.equals(datacenter))
        {
            candidates = new ArrayList<>(localDatacenterView);
        }
        else
        {
            // first check if we have a peer in the active view for the remote datacenter
            InetAddress remotePeer = remoteView.get(datacenter);
            if (remotePeer != null && !remotePeer.equals(filter))
            {
                candidates = new ArrayList<InetAddress>()  {{  add(remotePeer);  }};
            }
            else
            {
                // try the entire list of remote peers for the datacenter. if the only known peer in that datacenter
                // is the one we're filtering out, then fall back to use only the local datacenter peers
                Collection<InetAddress> allRemotes = endpointStateSubscriber.peers.get(datacenter);
                if (allRemotes.size() == 1 && allRemotes.contains(filter))
                    candidates = new ArrayList<>(localDatacenterView);
                else
                    candidates = new ArrayList<>(allRemotes);
            }
        }

        candidates.remove(localAddress);
        candidates.remove(filter);

        if (candidates.isEmpty())
            return Optional.empty();
        if (candidates.size() == 1)
            return Optional.of(candidates.get(0));
        Collections.shuffle(candidates, random);
        return Optional.of(candidates.get(0));
    }

    /**
     * Handle a neighbor connection request. If the message has a high priority, we must accept it.
     * If a low priority, check if we have space in the active view (for the peer's datacenter), and accept
     * the connect is there an open slot.
     */
    @VisibleForTesting
    void handleNeighborRequest(NeighborRequestMessage message)
    {
        // if the node is already in our active view, go ahead and send an ACCEPT message
        if (getPeers().contains(message.requestor))
        {
            messageSender.send(localAddress, message.requestor, new NeighborResponseMessage(localAddress, datacenter, Result.ACCEPT, message.neighborRequestsCount));
            return;
        }

        if (message.priority == Priority.LOW)
        {
            if ((message.datacenter.equals(datacenter) && endpointStateSubscriber.fanout(datacenter, datacenter) <= localDatacenterView.size())
                 || remoteView.containsKey(message.datacenter))
            {
                messageSender.send(localAddress, message.requestor, new NeighborResponseMessage(localAddress, datacenter, Result.DENY, message.neighborRequestsCount));
                return;
            }
        }

        addToView(message.requestor, message.datacenter);
        messageSender.send(localAddress, message.requestor, new NeighborResponseMessage(localAddress, datacenter, Result.ACCEPT, message.neighborRequestsCount));
    }

    /**
     * If the peer ACCEPTed the neighbor request, consider the bond good and add it to the
     * active view. Else, try sending a neighbor request to another peer, unless we're over the limit for request attempts.
     */
    @VisibleForTesting
    void handleNeighborResponse(NeighborResponseMessage message)
    {
        // TODO:JEB add tests

        if (message.result == Result.ACCEPT)
        {
            // if we get a duplicate response, or the peer has already found it's way into the active view, don't repocess
            if (getPeers().contains(message.requestor))
                return;
            addToView(message.requestor, message.datacenter);
        }
        else
        {
            int nextRequestCount = message.neighborRequestsCount + 1;
            if (nextRequestCount < MAX_NEIGHBOR_REQUEST_ATTEMPTS)
                sendNeighborRequest(Optional.of(message.requestor), message.datacenter);
            else
                logger.debug("neighbor request attempts exceeded. will wait for periodic task to connect to more peers.");
        }
    }

    /**
     * Remove the requestor from our active view, and, if it was actaully in our active view, try
     * to replace it with a node from the passive view.
     */
    @VisibleForTesting
    void handleDisconnect(DisconnectMessage message)
    {
        // TODO:JEB impl tests

        if (localDatacenterView.remove(message.requestor) || remoteView.remove(message.datacenter, message.requestor))
            sendNeighborRequest(Optional.of(message.requestor), message.datacenter);
    }

    void sendNeighborRequest(Optional<InetAddress> filtered, String datacenter)
    {
        // TODO:JEB impl tests

        // remove node from active view
        List<InetAddress> candidates;
        if (this.datacenter.equals(datacenter))
        {
            candidates = new ArrayList<>(endpointStateSubscriber.peers.get(datacenter));
            // filter out nodes already in the active view
            candidates.removeAll(localDatacenterView);
            candidates.remove(localAddress);
        }
        else
        {
            Collection<InetAddress> remotePeers = endpointStateSubscriber.peers.get(datacenter);
            if (remotePeers == null || remotePeers.isEmpty())
            {
                logger.debug("no more peers from remote datacenter " + datacenter);
                return;
            }
            candidates = new ArrayList<>(remotePeers);
        }

        if (filtered.isPresent())
            candidates.remove(filtered);
        if (candidates.isEmpty())
            return;
        Collections.shuffle(candidates, random);
        // TODO: handle case where local DC is empty, as we need to send high priority (preferrably to own DC)
        messageSender.send(localAddress, candidates.get(0), new NeighborRequestMessage(localAddress, datacenter, Priority.LOW, 0));
    }

    public Collection<InetAddress> getPeers()
    {
        List<InetAddress> peers = new ArrayList<>(localDatacenterView.size() + remoteView.size());
        peers.addAll(localDatacenterView);
        peers.addAll(remoteView.values());

        return peers;
    }

    public void register(PeerSamplingServiceListener listener)
    {
        listeners.add(listener);
    }

    public void unregister(PeerSamplingServiceListener listener)
    {
        listeners.remove(listener);
    }

    void peerUnavailable(InetAddress addr)
    {
        Optional<String> datacenter = endpointStateSubscriber.getDatacenter(addr);
        if (datacenter.isPresent())
            peerUnavailable(addr, datacenter.get());
    }

    /**
     * Utility for informing all listeners that a peer in the cluster is either unavailable or has been explicitly
     * marked down.
     */
    void peerUnavailable(InetAddress addr, String datacenter)
    {
        // TODO:JEB add tests and think about if we really need to sned neighbor requests --- or if this should call removeNode()
        // basically, rethink this flow...
        for (PeerSamplingServiceListener listener : listeners)
            listener.neighborDown(addr, datacenter);
        sendNeighborRequest(Optional.of(addr), datacenter);
    }

    /**
     * A check that should run periodically to ensure this node is properly connected to the peer sampling service.
     * For the local datacenter, this node should be connected the fanout number of nodes. For remote datacenters,
     * it should connected to, at the maximum, only one node. However, we could not be connected to a node in a remote
     * datacenter if that datacenter has a lesser number of nodes.
     */
    void checkConnectivity()
    {
        // TODO:JEB add tests

        Multimap<String, InetAddress> peers = endpointStateSubscriber.peers;
        for (String datacenter : peers.keySet())
        {
            if (datacenter.equals(this.datacenter))
            {
                int fanout = endpointStateSubscriber.fanout(this.datacenter, datacenter);
                if (localDatacenterView.size() < fanout)
                {
                    // TODO:JEB should we really send out a bunch of messages? or just one at a time,
                    // and gently ease the cluster up (as we know this method will be called periodically)
                    for (int i = fanout - localDatacenterView.size(); i > 0; i--)
                    {
                        // send neighbor request(s) to peers not already in view
                        // TODO:JEB impl me
                    }
                }
            }
            else if (!remoteView.containsKey(datacenter))
            {
                // only attempt to add peers from the remote datacenter if it has an equal or greater number of nodes.
                if (peers.get(this.datacenter).size() <= peers.get(datacenter).size())
                {
                    // send message to arbitrary peer in remote dc
                    // TODO:JEB impl me
                }
            }
        }
    }

    /**
     * Check to see if we've received at least one response to our join request. If not, attempt to join again.
     */
    void checkJoinStatus()
    {
        if (!hasJoined)
            join();
    }

    /**
     * Maintains an independent mapping of {datacenter->peer} of all the current members.
     * Members are allowed to participcate in gossip operations.
     */
    class EndpointStateSubscriber implements IEndpointStateChangeSubscriber
    {
        /**
         * The minimum size for the number of nodes in a local datacenter to be larger than
         * to use the natural log for the fanout value.
         */
        private static final int NATURAL_LOG_THRESHOLD = 16;

        /**
         * Internal mapping of all nodes to their respective datacenters.
         *
         * as current Gossiper is single-threaded, we can *probably* use the multimap
         * with a decent degree of certainty we'll be thread-safe
         */
        private final Multimap<String, InetAddress> peers;

        EndpointStateSubscriber()
        {
            peers = HashMultimap.create();
        }

        /**
         * Determine the fanout (number of nodes to contact) for the target datacenter.
         * If the target is different from the local datacenter, always returns 1. If it is
         * the same datacenter, and if the number of nodes if below some threshold, return
         * the number of nodes in the datacenter (to contact them all); else, if, there's a
         * large number of nodes in the local datacenter, return the natural log of the cluster.
         */
        public int fanout(String localDatacenter, String targetDatacenter)
        {
            if (!localDatacenter.equals(targetDatacenter))
                return 1;

            // TODO I think there's a problem here with the race of when we get notified, via gossip, of
            // nodes getting added to the cluser (and we update the EndpointStateListener.peers field)
            // versus when we get a request to join the PeerSamplingService.
            // mostly a problem of adding nodes, not removing (if we're over the fanout, not big deal)

            Collection<InetAddress> localPeers = peers.get(localDatacenter);
            // if there are no other peers in this datacenter, default to 1
            if (localPeers.isEmpty())
                return 1;

            int localPeerCount = localPeers.size();
            if (localPeerCount >= NATURAL_LOG_THRESHOLD)
                return (int)Math.ceil(Math.log(localPeerCount));
            return localPeerCount;
        }

        public Optional<String> getDatacenter(InetAddress addr)
        {
            // hoping there's a more efficient way of getting the datacenter for a node...
            for (Map.Entry<String, Collection<InetAddress>> entry : peers.asMap().entrySet())
            {
                if (entry.getValue().contains(addr))
                    return Optional.of(entry.getKey());
            }
            return Optional.empty();
        }

        /**
         * Side-door for tests to insert values into the peers map.
         */
        @VisibleForTesting
        void add(InetAddress peer, String datacenter)
        {
            peers.put(datacenter, peer);
        }

        public void onJoin(InetAddress endpoint, EndpointState epState)
        {
            // nop
        }

        public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
            // nop
        }

        public void onChange(InetAddress addr, ApplicationState state, VersionedValue value)
        {
            // double-check if we already know about the peer and it's datacenter
            if (state == ApplicationState.DC)
                newPeer(addr, value.value);
        }

        boolean newPeer(InetAddress peer, String datacenter)
        {
            if (peers.containsValue(peer))
                return false;

            peers.put(datacenter, peer);
            return true;
        }

        public void onAlive(InetAddress endpoint, EndpointState state)
        {
            VersionedValue datacenter = state.getApplicationState(ApplicationState.DC);
            if (datacenter == null)
                return;
            newPeer(endpoint, datacenter.value);
        }

        public void onDead(final InetAddress endpoint, EndpointState state)
        {
            VersionedValue datacenter = state.getApplicationState(ApplicationState.DC);
            if (datacenter == null)
                return;
            if (peers.remove(datacenter.value, endpoint))
                executorService.submit(() -> peerUnavailable(endpoint, datacenter.value));
        }

        public void onRemove(final InetAddress endpoint)
        {
            Optional<String> datacenter = getDatacenter(endpoint);
            if (!datacenter.isPresent())
                return;
            if (peers.remove(datacenter.get(), endpoint))
                executorService.submit(() -> peerUnavailable(endpoint, datacenter.get()));
        }

        public void onRestart(InetAddress endpoint, EndpointState state)
        {
            onAlive(endpoint, state);
        }
    }

    /*
        method for IFailureDetectionEventListener
     */
    public void convict(final InetAddress addr, double phi)
    {
        // TODO: not entirely clear if we should listen to the whims of the FD, but go with it for now
        executorService.submit(() -> peerUnavailable(addr));
    }

    class ClusterConnectivityChecker implements Runnable
    {
        public void run()
        {
            // because this class will be executed from the scheduled tasks thread, move the actaul execution
            // into the primary exec pool for this class
            executorService.submit(() -> checkConnectivity());
        }
    }

    class JoinChecker implements Runnable
    {
        public void run()
        {
            // because this class will be executed from the scheduled tasks thread, move the actaul execution
            // into the primary exec pool for this class
            executorService.submit(() -> checkJoinStatus());
        }
    }
}
