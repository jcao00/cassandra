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
import com.google.common.base.Preconditions;
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
import org.apache.cassandra.utils.Pair;

/**
 * An implementation of the HyParView paper, Leitao, et al, 2008
 * http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf. Briefly, HyParView is a {@code PeerSamplingService}
 * that maintains a subset view of the peers in cluster that are to be used by dependent modules
 * (for example, upper-layer gossip broadcast components). HyParView uses a loose coordination protocol
 * to ensure all nodes are included in at least one peer's view (ensuring complete coverage). Also, each node
 * maintains a symmetric relationship with each peer in it's active view: if node A has node B in it's active view,
 * B should have A in it's own active view.
 *
 * However, we take some deviations with our implementation. HyParView requires two views of peers
 * in the cluster: an active view and a passive view. We maintain the active view in this implementation,
 * but we can piggy-back off the existing knowledge of the full cluster as a replacement
 * for all the mechanics of maintaining the passive view (see 4.4 of the paper, especially
 * the part on the SHUFFLE messages). We still fulfill the needs of the passive view (having a backlog
 * of peers to use as a backup), but save ourselves some code/network traffic/extra functionality.
 *
 * State machine:
 * A nice way to think about the active view is to consider it like a state machine, wherein each node controls how peers
 * are allowed into node's (local) active view. We can consider the state machine with three states:
 * - start: everything begins here, and directly transitions to ...
 * - not in active view: a peer can transition to this state either by starting up or sending or receiving a DISCONNECT message
 * - in active view: a node can transition into this state by receiving a JOIN, JOIN_REQUEST, and so on (read the code and paper for exact details).
 *
 * However, the complications arise as this state machine reflects a distributed system, with asynchrnous messaging and processes
 * that are allowed to fail/restart/bounce. Thus, we need to keep rules as to how the transitions are allowed to happen.
 * Each node needs to maintain data about disconnects sent and received, and, further, disconnect sends also need to track
 * the last seen messageId of the peer to which it is sending this disconnect. We keep this data around so we can establish
 * some "happens before" facts so that we know logically when one event preceeds another and if a given peer has seen a disconnect.
 *
 * The basic rule is a node should not transition a peer to the active view state if the peer's "connect" message
 * (JOIN, JOIN_RESPONSE, and so on) cannot prove it has seen all DISCONNECT messages sent to it by the node.
 * This is expressed by sending along in the connect message the highest DISCONNECT message ID
 * the peer has seen from the node it is attempting to connect to. The recipient will check against it's own maintained
 * collection of sent DISCONNECT message IDs (capturing the highest DISCONNECT message ID sent to each peer),
 * and if the connect message's "last seen disconnect message ID" is not less than the local value,
 * then the peer has seen all the DISCONNECTs and we can safely to transition it to the local active view.
 *
 * As we are not persisting the DISCONNECT message IDs (sent or received), so when a node bounces it loses the history
 * of the DISCONNECTs it has either sent or received. What this means is that on restart, we can't send any previously known
 * DISCONNECT message IDs, and thus a peer receiving the connect message might decline the connect request as it would be lacking the
 * proper DISCONNECT message ID. To alleviate that, each {@code HPVMessageId} contains an "epoch" field, which is incremented
 * on each restart of a node (by reading from, updating, and writing a value from stable storage). Thus, a node receiving
 * a connect request with a higher epoch than the last message ID from the peer (recorded with the disconnect sent to the peer)
 * can safely assume the requestor bounced and thus consider it for inclusion in the active view, even though the request itself
 * lacks the previous disconect data. We assume, due to the higher epoch in the request message, the disconnect
 * "happened before" the new connect attempt.
 *
 * To transition out of the active view state, and into the "not in active view" state, is much simpler:
 * 1) a node evicts a peer from the active view, and thus sends a DISCONNECT message
 * 2) a node receives a DISCONNECT message from a peer, and the message's ID is higher than the locally recorded DISCONNECT message ID
 * for the peer.
 * 3) we are informed of a peer being decommissioned, removed, shutdown, and thus via the peer's state changes, or becomes unavailable
 * (notifed via a failure detector).
 *
 * Datacenter concerns:
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
 * With that restriction, though, is the requirement that all mutations to state *must* happen on the
 * primary (gossip) thread.
 */
public class HyParViewService implements PeerSamplingService, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewService.class);
    static final int MAX_NEIGHBOR_REQUEST_ATTEMPTS = 2;

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
     * logical datacenter this node is executing in.
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

    private final HPVMessageId.IdGenerator idGenerator;

    /**
     * Capture the highest message id received from each peer.
     */
    private final Map<InetAddress, HPVMessageId> highestSeenMessageIds;

    /**
     * Capture the id of the last message DISCONNECT either received from or sent to a peer.
     */
    private final Map<InetAddress, Disconnects> lastDisconnect;

    @VisibleForTesting
    MessageSender messageSender;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduler;

    /**
     * Simple flag to indicate if we've received at least one response to a join request. Reset each time
     * {@code HyParViewService#join} is called.
     */
    private volatile boolean hasJoined;

    public HyParViewService(InetAddress localAddress, String datacenter, long epoch, SeedProvider seedProvider, int activeRandomWalkLength)
    {
        this.localAddress = localAddress;
        this.datacenter = datacenter;
        this.seedProvider = seedProvider;
        this.activeRandomWalkLength = activeRandomWalkLength;

        idGenerator = new HPVMessageId.IdGenerator(epoch);
        highestSeenMessageIds = new HashMap<>();
        lastDisconnect = new HashMap<>();
        random = new Random();
        listeners = new HashSet<>();
        endpointStateSubscriber = new EndpointStateSubscriber();

        // explicitly using a LinkedList here - makes removals simpler
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
            logger.info("no seeds left in the seed list (after removing this node), will wait for other nodes to join to start gossiping");
            return;
        }

        hasJoined = false;
        Collections.shuffle(seeds, random);
        InetAddress seed = seeds.get(0);
        messageSender.send(seed, new JoinMessage(idGenerator.generate(), localAddress, datacenter));
        scheduler.schedule(new JoinChecker(), 10, TimeUnit.SECONDS);
    }

    /**
     * Handle incoming messages from peers.
     */
    public void receiveMessage(HyParViewMessage message)
    {
        updateLastMessageSeen(message.sender, message.messgeId);

        try
        {
            switch (message.getMessageType())
            {
                case JOIN:
                    handleJoin((JoinMessage) message);
                    break;
                case JOIN_RESPONSE:
                    handleJoinResponse((JoinResponseMessage) message);
                    break;
                case FORWARD_JOIN:
                    handleForwardJoin((ForwardJoinMessage) message);
                    break;
                case NEIGHBOR_REQUEST:
                    handleNeighborRequest((NeighborRequestMessage) message);
                    break;
                case NEIGHBOR_RESPONSE:
                    handleNeighborResponse((NeighborResponseMessage) message);
                    break;
                case DISCONNECT:
                    handleDisconnect((DisconnectMessage) message);
                    break;
                default:
                    throw new IllegalArgumentException("Unhandled hyparview message type: " + message.getMessageType());
            }
        }
        catch (Exception e)
        {
            logger.error(String.format("error while processing message %s", message), e);
        }
    }

    private void updateLastMessageSeen(InetAddress sender, HPVMessageId messgeId)
    {
        HPVMessageId previousEntry = highestSeenMessageIds.get(sender);
        if (previousEntry == null || messgeId.compareTo(previousEntry) > 0)
            highestSeenMessageIds.put(sender, messgeId);
    }

    /**
     * Handle an incoming request message to JOIN the HyParView subsystem. When we receive a join,
     * we always add that node to our active view, possibly disconnecting from a node currently in the active view
     * (if the active view size limit is exceeded). Then we send out a FORWARD_JOIN message to all peers in the active view.
     */
    @VisibleForTesting
    void handleJoin(JoinMessage message)
    {
        if (!addToView(message))
            return;

        messageSender.send(message.sender, new JoinResponseMessage(idGenerator.generate(), localAddress, datacenter,
                                                                   lastDisconnectMsgId(message.sender)));

        Collection<InetAddress> peers = getPeers();
        peers.remove(message.sender);

        if (!peers.isEmpty())
        {
            ForwardJoinMessage msg = new ForwardJoinMessage(idGenerator.generate(), localAddress, datacenter, message.sender, message.datacenter,
                                                            activeRandomWalkLength, message.getOriginatorMessageId());
            for (InetAddress peer : peers)
                messageSender.send(peer, msg);
        }
    }

    private HPVMessageId lastDisconnectMsgId(InetAddress peer)
    {
        Disconnects disconnects = lastDisconnect.get(peer);
        return disconnects == null ? null : disconnects.fromPeer;
    }

    /**
     * Add a peer to the active view. Check to see if we've recieved a DISCONNECT message with a higher message id, and, if so,
     * reject this add.
     */
    boolean addToView(HyParViewMessage message)
    {
        return hasSeenDisconnect(message, lastDisconnect) &&
               addPeerToView(message.getOriginator(), message.getOriginatorDatacenter());
    }

    /**
     * Check if the peer has seen the last DISCONNECT message id we have sent it. Also checks to make sure we have not
     * received a DISONNECT from the peer with a higher messgage id than the is in the current message (helps protect
     * against duplicate messages).
     *
     * There's some rules as per each message type as to how it is processed:
     *
     * - JOIN, NEIGHBOR_REQEUST, NEIGHBOR_RESPONSE - the easier case. The sender must send along the highest DISCONNECT
     * message id it has seen from the recipient. That way the recipient can prove the sender has seen the last DISCONNECT
     * sent to it. If the sender is bounced (and either received or did not recieve the DISCONNECT from the recipient),
     * it will no longer retain any DISCONNECT data, so therefore the recipient must check the epoch field of the incoming
     * "connect" message against the last message ID seen when the DISCONNECT was sent. The epoch field is incremented only
     * when a peer restarts, so thus we assume a node bounced and we can ignore the absence of DISCONNECT data.
     *
     * JOIN_RESPONSE, FORWARD_JOIN - the trickier case, the above conditions apply, plus these.
     * As each of these could have been sent at the end of a chain of forward joins, we need to retain the originating peer's
     * (the node that sent the JOIN) message id in the message chain. As JOIN only happens at node start, we can assume it has
     * no DISCONNECT data that needs to be passed around (which would have to be sent/copied in the JOIN, FORWARD_JOIN, and
     * JOIN_RESPONSE messages). Since we do not store DISCONNECT information, it will be lost after shutdown, and thus will
     * be null on startup and nothing to pass around.
     */
    @VisibleForTesting
    boolean hasSeenDisconnect(HyParViewMessage message, Map<InetAddress, Disconnects> lastDisconnect)
    {
        Disconnects disconnects = lastDisconnect.get(message.getOriginator());
        if (disconnects == null)
            return true;
        if (!disconnects.hasSeenMostRecentFromPeer(message.getOriginatorMessageId()))
            return false;

        if (!disconnects.hasSeenMostRecentToPeer(message.getLastDisconnect(), message.getOriginatorMessageId()))
            return false;

        return true;
    }

    private boolean addPeerToView(InetAddress peer, String datacenter)
    {
        if (peer.equals(localAddress))
            return false;

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
        return added;
    }

    @VisibleForTesting
    boolean addPeerToView(InetAddress peer, String datacenter, HPVMessageId messageId)
    {
        highestSeenMessageIds.put(peer, messageId);
        return addPeerToView(peer, datacenter);
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
        logger.info(String.format("%s adding %s to local active view", localAddress, peer));
        if (localDatacenterView.size() > endpointStateSubscriber.fanout(datacenter, datacenter))
            expungeNode(localDatacenterView.removeFirst(), datacenter);
        return true;
    }

    private void expungeNode(InetAddress peer, String datacenter)
    {
        HPVMessageId id = idGenerator.generate();
        logger.info(String.format("%s removing %s from active view (msgId: %s) due to new node in view", localAddress, peer, id));

        HPVMessageId previousMessageId = highestSeenMessageIds.get(peer);
        Preconditions.checkNotNull(previousMessageId, String.format("%s has no previous message id from peer %s", localAddress, peer));
        Disconnects disconnects = lastDisconnect.get(peer);
        if (disconnects == null)
            disconnects = new Disconnects(null, Pair.create(id, previousMessageId));
        else
            disconnects = disconnects.withNewToPeerMsgId(Pair.create(id, previousMessageId));

        lastDisconnect.put(peer, disconnects);
        messageSender.send(peer, new DisconnectMessage(id, localAddress, this.datacenter));

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

        logger.info(String.format("%s adding %s to remote active view", localAddress, peer));
        remoteView.put(datacenter, peer);
        if (existing != null)
            expungeNode(existing, datacenter);
        return true;
    }

    @VisibleForTesting
    boolean removePeer(InetAddress peer, String datacenter)
    {
        if (localDatacenterView.remove(peer) || remoteView.remove(datacenter, peer))
        {
            expungeNode(peer, datacenter);
            return true;
        }
        return false;
    }

    /**
     * Handle a response to a join request. Mostly just need to create the symmetric connection (or entry)
     * in our active view, and disable any join checking mechanisms.
     */
    @VisibleForTesting
    void handleJoinResponse(JoinResponseMessage message)
    {
        if (addToView(message))
            hasJoined = true;
    }

    /**
     * Handle an incoming forward join message. If the message's time-to-live is greater than 0,
     * forward the message to a node from the active view (avoiding sending back to the peer that forwarded
     * to us). If the message's time-to-live is 0, or we have <= 1 nodes in local DC's view (or remote datacenter view is empty),
     * add the peer to the active view and respond back to requesting node.
     *
     * Note: we allow the TTL go below zero (-1 at the least) on the chance that if the (TTL == 0) node doesn't accept,
     * we'll forward it along one more time to see if it gets lucky.
     */
    @VisibleForTesting
    void handleForwardJoin(ForwardJoinMessage message)
    {
        int nextTTL = message.timeToLive - 1;
        boolean added = false;
        if (message.getOriginatorDatacenter().equals(datacenter))
        {
            if (nextTTL <= 0 || localDatacenterView.size() <= 1)
            {
                if (addToView(message))
                    added = true;
            }
        }
        else
        {
            if (nextTTL <= 0 || !remoteView.containsKey(message.getOriginatorDatacenter()))
            {
                if (addToView(message))
                    added = true;
            }
        }

        if (added)
        {
            // capture the originator's messageId in case we need to send a DISCONNECT to the peer before
            // it ever sends us a message directly.
            updateLastMessageSeen(message.getOriginator(), message.getOriginatorMessageId());
            messageSender.send(message.originator, new JoinResponseMessage(idGenerator.generate(), localAddress, datacenter,
                                                                           lastDisconnectMsgId(message.originator)));
        }
        else if (nextTTL >= 0)
        {
            // make sure we don't send a FORWARD_JOIN back to the node who is trying to join, and avoid sending it to the node
            // who sent it to us (unlesss there's no other peer in the active view to contact).
            List<InetAddress> filter = new LinkedList<>();
            filter.add(message.sender);
            filter.add(message.getOriginator());
            Optional<InetAddress> peer = getActivePeer(filter, message.datacenter);
            InetAddress addr = peer.orElseGet(() -> message.sender);
            messageSender.send(addr, new ForwardJoinMessage(idGenerator.generate(), localAddress, datacenter,
                                                            message.originator, message.originatorDatacenter, nextTTL,
                                                            message.getOriginatorMessageId()));
        }
    }


    /**
     * Find a random target node in the active view, in the requested datacenter. If the datacenter is remote,
     * first try the peer that is in the active view for that datacenter. If there is no active peer,
     * try to select another node from that datacenter (assuming we want to keep the forwarding in that datacenter).
     * If there's no other known nodes in that datacenter, fall back to forwarding to a node in the current datacenter.
     */
    Optional<InetAddress> getActivePeer(Collection<InetAddress> filter, String datacenter)
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
                allRemotes.removeAll(filter);
                if (allRemotes.isEmpty())
                    candidates = new ArrayList<>(localDatacenterView);
                else
                    candidates = new ArrayList<>(allRemotes);
            }
        }

        candidates.remove(localAddress);
        candidates.removeAll(filter);

        if (candidates.isEmpty())
            return Optional.empty();
        if (candidates.size() == 1)
            return Optional.of(candidates.get(0));
        Collections.shuffle(candidates, random);
        return Optional.of(candidates.get(0));
    }

    /**
     * Handle a neighbor connection request. If the message has a high priority, it must be accepted.
     * If a low priority, check if there is space in the active view (for the peer's datacenter), and accept
     * the connect is there an open slot.
     */
    @VisibleForTesting
    void handleNeighborRequest(NeighborRequestMessage message)
    {
        // if the node is already in our active view, go ahead and send an ACCEPT message
        if (getPeers().contains(message.sender))
        {
            messageSender.send(message.sender, new NeighborResponseMessage(idGenerator.generate(), localAddress, datacenter,
                                                                           Result.ACCEPT, message.neighborRequestsCount, lastDisconnectMsgId(message.sender)));
            return;
        }

        if (message.priority == Priority.LOW)
        {
            if ((message.datacenter.equals(datacenter) && endpointStateSubscriber.fanout(datacenter, datacenter) <= localDatacenterView.size())
                 || remoteView.containsKey(message.datacenter))
            {
                messageSender.send(message.sender, new NeighborResponseMessage(idGenerator.generate(), localAddress, datacenter,
                                                                               Result.DENY, message.neighborRequestsCount, lastDisconnectMsgId(message.sender)));
                return;
            }
        }

        Result result = addToView(message) ? Result.ACCEPT : Result.DENY;
        messageSender.send(message.sender, new NeighborResponseMessage(idGenerator.generate(), localAddress, datacenter,
                                                                       result, message.neighborRequestsCount, lastDisconnectMsgId(message.sender)));
    }

    /**
     * If the peer ACCEPTed the neighbor request, add the peer to the active view. Else, try sending another neighbor request
     * to a different peer, unless we're over the limit for request attempts.
     */
    @VisibleForTesting
    void handleNeighborResponse(NeighborResponseMessage message)
    {
        // if we get a duplicate response, or the peer has already found it's way into the active view, don't reprocess
        if (getPeers().contains(message.sender))
            return;

        if (message.result == Result.ACCEPT && addToView(message))
            return;

        int nextRequestCount = message.neighborRequestsCount + 1;
        if (nextRequestCount < MAX_NEIGHBOR_REQUEST_ATTEMPTS && shouldSendNeighborRequest(datacenter))
            sendNeighborRequest(Optional.of(message.sender), message.datacenter, nextRequestCount);
        else
            logger.debug("neighbor request attempts exceeded. will wait for periodic task to connect to more peers.");
    }

    /**
     * Only send a NEIGHBOR_REQUEST if:
     * - {@code datacenter} is the local datacenter, and the active view is not full.
     * - {@code datacenter} is a remote datacenter, and the number of nodes in the local datacenter
     * is less than or equal to the number in the remote datacenter.
     */
    @VisibleForTesting
    boolean shouldSendNeighborRequest(String datacenter)
    {
        return datacenter.equals(this.datacenter)
            ? endpointStateSubscriber.fanout(datacenter, datacenter) > localDatacenterView.size()
            : endpointStateSubscriber.comapreDatacenterSizes(this.datacenter, datacenter) <= 0;
    }

    /**
     * Remove the requestor from our active view, and, if it was actaully in our active view, try
     * to replace it with a node from the passive view from the same datacenter as the disconnecting peer.
     */
    void handleDisconnect(DisconnectMessage message)
    {
        Disconnects disconnects = lastDisconnect.get(message.getOriginator());
        if (disconnects == null)
            lastDisconnect.put(message.getOriginator(), new Disconnects(message.messgeId, null));
        else if (disconnects.hasSeenMostRecentFromPeer(message.messgeId))
            lastDisconnect.put(message.getOriginator(), disconnects.withNewFromPeerMsgId(message.messgeId));
        else
            return; // we received some duplicate or older message - ignore

        logger.info(String.format("%s removing %s", localAddress, message));

        boolean removed = localDatacenterView.remove(message.sender) || remoteView.remove(message.datacenter, message.sender);
        if (removed && shouldSendNeighborRequest(message.datacenter))
            sendNeighborRequest(Optional.of(message.sender), message.datacenter);

        //TODO:JEB check to see if we need to notify listeners
    }

    /**
     * Attempt to send a neighbor request to the given datacenter.
     *
     * @param filtered An additional peer to filter out; for example, if we received a negative response from a peer, and
     *                 cannot send a message back to it but need to select another.
     * @param datacenter The datacenter from which to select a peer.
     */
    void sendNeighborRequest(Optional<InetAddress> filtered, String datacenter)
    {
        sendNeighborRequest(filtered, datacenter, 0);
    }

    /**
     * Attempt to send a neighbor request to the given datacenter.
     *
     * @param filtered An additional peer to filter out; for example, if we received a negative response from a peer, and
     *                 cannot send a message back to it but need to select another.
     * @param datacenter The datacenter from which to select a peer.
     * @param messageRetryCount The retry count to set into the message.
     */
    void sendNeighborRequest(Optional<InetAddress> filtered, String datacenter, int messageRetryCount)
    {
        Optional<InetAddress> peer = getPassivePeer(filtered, datacenter);
        if (!peer.isPresent())
            return;

        messageSender.send(peer.get(), new NeighborRequestMessage(idGenerator.generate(), localAddress, this.datacenter,
                                                                  determineNeighborPriority(datacenter), messageRetryCount,
                                                                  lastDisconnectMsgId(peer.get())));
    }

    /**
     * Filter the known peers in the datacenter by entries in the active view, and if any peers remain, select an arbitrary peer.
     * Exclude the filter peer, as well.
     */
    Optional<InetAddress> getPassivePeer(Optional<InetAddress> filtered, String datacenter)
    {
        List<InetAddress> candidates = new ArrayList<>(endpointStateSubscriber.peers.get(datacenter));
        if (this.datacenter.equals(datacenter))
        {
            // filter out nodes already in the active view
            candidates.removeAll(localDatacenterView);
            candidates.remove(localAddress);
        }
        else
        {
            if (remoteView.containsKey(datacenter))
                candidates.remove(remoteView.get(datacenter));
        }

        if (filtered.isPresent())
            candidates.remove(filtered.get());
        if (candidates.isEmpty())
            return Optional.empty();
        Collections.shuffle(candidates, random);
        return Optional.of(candidates.get(0));
    }

    /**
     * Normally, the priority is set to LOW unless there are no peers in the active view, then it is set to HIGH.
     */
    Priority determineNeighborPriority(String datacenter)
    {
        return getPeers().isEmpty() || (datacenter.equals(this.datacenter) && localDatacenterView.isEmpty())
               ? Priority.HIGH : Priority.LOW;
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
        for (PeerSamplingServiceListener listener : listeners)
            listener.neighborDown(addr, datacenter);

        if (shouldSendNeighborRequest(datacenter))
            sendNeighborRequest(Optional.of(addr), datacenter);
    }

    /**
     * A check that should run periodically to ensure this node is properly connected to the peer sampling service.
     * For the local datacenter, this node should be connected the fanout number of nodes. For remote datacenters,
     * it should connected to, at the maximum, only one node. However, we could not be connected to a node in a remote
     * datacenter if that datacenter has a lesser number of nodes.
     */
    void checkFullActiveView()
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
                    List<InetAddress> localPeers = new ArrayList<>(endpointStateSubscriber.peers.get(this.datacenter));
                    localPeers.removeAll(localDatacenterView);
                    localPeers.remove(localAddress);
                    Collections.shuffle(localPeers, random);
                    for (InetAddress peer : localPeers.subList(0, fanout - localDatacenterView.size()))
                    {
                        // don't let these messages spin in retry attempts - just try once, because we can try again on the next round
                        int count = MAX_NEIGHBOR_REQUEST_ATTEMPTS - 1;
                        NeighborRequestMessage msg = new NeighborRequestMessage(idGenerator.generate(), localAddress, this.datacenter, determineNeighborPriority(datacenter), count,
                                                                                lastDisconnectMsgId(peer));
                        messageSender.send(peer, msg);
                    }
                }
            }
            else if (!remoteView.containsKey(datacenter))
            {
                // only attempt to add peers from the remote datacenter if it has an equal or greater number of nodes.
                if (endpointStateSubscriber.comapreDatacenterSizes(this.datacenter, datacenter) <= 0)
                    sendNeighborRequest(Optional.<InetAddress>empty(), datacenter);
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

    @VisibleForTesting
    public Collection<InetAddress> getLocalDatacenterView()
    {
        return localDatacenterView;
    }

    @VisibleForTesting
    public Map<String, InetAddress> getRemoteView()
    {
        return remoteView;
    }

    @VisibleForTesting
    public InetAddress getLocalAddress()
    {
        return localAddress;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(256);
        sb.append(localAddress).append(" (").append(datacenter).append("): local view = ").append(localDatacenterView);
        sb.append(" remote view = ").append(remoteView);
        return sb.toString();
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
        private static final int NATURAL_LOG_THRESHOLD = 10;

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
         * Compare the number of nodes in one datacenter to the number of nodes in another. Behaves like a typical
         * {@link Comparable#compareTo(Object)} method.
         */
        int comapreDatacenterSizes(String dc1, String dc2)
        {
            return peers.get(dc1).size() - peers.get(dc2).size();
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
        // not entirely clear if we should listen to the whims of the FD, but go with it for now
        executorService.submit(() -> peerUnavailable(addr));
    }

    class ClusterConnectivityChecker implements Runnable
    {
        public void run()
        {
            // because this class will be executed from the scheduled tasks thread, move the actaul execution
            // into the primary exec pool for the owning class
            executorService.submit(() -> checkFullActiveView());
        }
    }

    class JoinChecker implements Runnable
    {
        public void run()
        {
            // because this class will be executed from the scheduled tasks thread, move the actaul execution
            // into the primary exec pool for the owning class
            executorService.submit(() -> checkJoinStatus());
        }
    }

    /**
     * Holds disconnect data received from/sent to a given peer.
     */
    static class Disconnects
    {
        /**
         * Last disconnect message id sent to a peer.
         */
        final HPVMessageId fromPeer;

        /**
         * Last disconnect message id sent to a peer and the last seen message ID from that peer when the DISCONNECT was sent.
         */
        final Pair<HPVMessageId, HPVMessageId> toPeer;

        Disconnects(HPVMessageId fromPeer, Pair<HPVMessageId, HPVMessageId> toPeer)
        {
            if (toPeer != null)
            {
                Preconditions.checkNotNull(toPeer.left);
                Preconditions.checkNotNull(toPeer.right);
            }
            this.fromPeer = fromPeer;
            this.toPeer = toPeer;
        }

        /**
         * create a new instance with an updated fromPeer value.
         */
        Disconnects withNewFromPeerMsgId(HPVMessageId fromMsgId)
        {
            return new Disconnects(fromMsgId, toPeer);
        }

        /**
         * create a new instance with an updated toPeer value.
         */
        Disconnects withNewToPeerMsgId(Pair<HPVMessageId, HPVMessageId> toMsgId)
        {
            return new Disconnects(fromPeer, toMsgId);
        }

        /**
         * Test if the parameter message ID is higher than the currently recorded fromPeer value.
         */
        boolean hasSeenMostRecentFromPeer(HPVMessageId msgId)
        {
            return fromPeer == null || msgId.compareTo(fromPeer) > 0;
        }

        /**
         * Test if the parameter message ID is higher than the currently recorded toPeer value. If the {@code disconnectMessageId}
         * is null (most likely because the peer restarted and has no recorded DISCONNECT information), then check that
         * the {@code messageId}'s epoch is higher (to prove that it bounced). Else, the peer has not received our most
         * recent DISCONNECT.
         */
        boolean hasSeenMostRecentToPeer(HPVMessageId disconnectMessageId, HPVMessageId messageId)
        {
            if (toPeer == null)
                return true;

            if (disconnectMessageId == null)
                return messageId.epochOnlyCompareTo(toPeer.right) > 0;

            return disconnectMessageId.compareTo(toPeer.left) >= 0;
        }

        public String toString()
        {
            return String.format("fromPeer %s, toPeer %s", fromPeer, toPeer);
        }
    }
}
