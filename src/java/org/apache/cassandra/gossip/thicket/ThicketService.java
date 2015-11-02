package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gossip.BroadcastService;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;

//TODO:JEB add more top-level documentation!!!!!!
/**
 * An implementation of <a href="http://asc.di.fct.unl.pt/~jleitao/pdf/srds10-mario.pdf">
 * Thicket: A Protocol for Maintaining Multiple Trees in a P2P Overlay</a>.
 * A thorough treatment is obviously in the paper, but sumamrized here. Thicket uses an underlying {@link PeerSamplingService}
 * to provide a view of the cluster from which it constructs dynamic spanning trees, upon which messages are broadcast gossip-style.
 * Thicket attempts to have each node act as an interior node in at most one of the n trees of the cluster; we will have a tree
 * per-node, so n is equal to the number of nodes in the cluster.
 *
 * Thicket maintains a set of active peers and a set of backup peers; initially all peers are in the backup set. The active set
 * maintains the collection of peers that messages are broadcast to (or received from), while the backup peers is used for populating
 * the active peers if it is not at capacity. The peers in the two sets are largely derived from the view provided by the PeerSamplingService.
 *
 * Messages:
 * - DATA - encapsulates the broadcast payload to be sent to the peers in the cluster.
 * - SUMMARY - a periodic broadcast to the peers in the backup set of all the recently received DATA message ids.
 * - GRAFT - sent when a node wants to repair a broken branch of the spanning tree, and connecting to the receiving peer
 * will heal the tree.
 * - PRUNE - sent by a node when it wants to be removed from the peer's active set (and put into it's backup set). Sent when a node
 * receives a duplicate DATA message or is declining a GRAFT request.
 *
 * State machine
 * A nice way to think about the active and passive peers sets is to consider them like a state machine, wherein each node controls how peers
 * are allowed into node's (local) peers sets. We can consider a state machine for peers with four states:
 * - START - The beginning state. Any peers to be added to the thicket views are learned about from the underlying PeerSamplingService,
 * through either a call to {@link PeerSamplingService#getPeers()} or a neighborUp notification, and then transition directly
 * to the BACKUP state.
 * - BACKUP - A peer is stored in the backup peers set, and is sent SUMMARY messages as part of tree repair. A peer can be transitioned
 * into this state from the ACTIVE state when we receive PRUNE message from the peer. The PRUNE could be due to the peer receiving a
 * duplicate message, rejecting a GRAFT request due to it's load, and so on.
 * - ACTIVE - A node is stored in the active peers set, and sends/receives broadcast messages. A peer can be transitioned to the
 * ACTIVE state when it receives GRAFT or GRAFT_RESPONSE message (sent as part of the SUMMARY tree repair process).
 * - REMOVED - The peer has been removed both views. This happen when the underlying PeerSamplingService send a neighborDown notification,
 * or the current node is shutting down.
 *
 *
 * Tree healing (SUMMARY)
 */
public class ThicketService implements BroadcastService, PeerSamplingServiceListener
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketService.class);

    /**
     * The time limit to retain message ids (for SUMMARY purposes), per tree-root.
     */
    private static final long MESSAGE_ID_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(40, TimeUnit.SECONDS);

    /**
     * The braodcast address of the local node. We memoize it here to avoid a hard dependency on FBUtilities.
     */
    private final InetAddress localAddress;

    @VisibleForTesting
    final MessageSender<ThicketMessage> messageSender;

    private PeerSamplingService peerSamplingService;

    /**
     * An executor pool to ensure all (mutation) behaviors are executed in a single thread of execution.
     */
    private final ExecutorService executor;

    /**
     * A service to use for scheduling tasks. Memoized here to avoid dependencies on other internal cassandra components.
     */
    private final ScheduledExecutorService scheduledTasks;
    private GossipMessageId.IdGenerator idGenerator;

    private boolean executing;

    /**
     * Mapping of a message originator's address (the tree root) to the downstream branch peers from this node.
     *
     * "for each tree T rooted at R, here is set the downstream peers P"
     */
    private final Multimap<InetAddress, InetAddress> broadcastPeers;

    // fanout < maxLoad < backupPeers.size()
    /**
     * A common set of backup peers to be used in determining downstream broadcast peers.
     */
    private final Collection<InetAddress> backupPeers;

    private final Map<String, BroadcastServiceClient> clients;

    /**
     * A reference to the future that is executing the {@link SummarySender}, so that it an be cancelled, if needed.
     */
    private ScheduledFuture<?> summarySender;

    /**
     * A reference to the future that is executing the {@link MissingMessagesTimer}, so that it an be cancelled, if needed.
     */
    private ScheduledFuture<?> missingMessagesResolver;

    /**
     * Collection of the most recently seen message ids. Entries will be published in SUMMARY messages, then moved to
     * {@code ThicketService#messagesLedger}, and this list will be cleared for next batch of incoming messages.
     */
    private final List<TimestampedMessageId> receivedMessages;

    /**
     * A log of the message ids after they've been published to peers in SUMMARY messages.
     * We keep them here for a limited time so that we can successfully match against incoming SUMMARY messages that we have indeed
     * seen the message ids. However, we can't keep the message ids around in memory forever, so we have to expunge them
     * after some time.
     */
    private final List<TimestampedMessageId> messagesLedger;

    /**
     * Record of message ids that have been reported to this node via SUMMARY messages, but which we have not seen.
     * (In the thicket paper, this data structure is referred to as the 'announcements' set.). Missing messages are grouped by the
     * tree root to which the messages belong, and because branching is unique per-tree, we need to identify uniquely
     * which tree is broken/partitioned/unhappy so we can repair the appropriate tree.
     */
    private final List<MissingMessges> missingMessages;

    /**
     * For each each peer, record it's {@link LoadEstimate} - the count of peers, per tree, that the node forwards broadcasted messages to.
     * This value is used when GRAFTing branches back onto a broken tree.
     */
    private final Multimap<InetAddress, LoadEstimate> loadEstimates;

    /**
     * Count of the messages this node has broadcasted as a tree-root.
     */
    private final AtomicInteger broadcastedMessages = new AtomicInteger();

    public ThicketService(InetAddress localAddress, MessageSender<ThicketMessage> messageSender, ExecutorService executor, ScheduledExecutorService scheduledTasks)
    {
        this.localAddress = localAddress;
        this.messageSender = messageSender;
        this.executor = executor;
        this.scheduledTasks = scheduledTasks;

        clients = new HashMap<>();
        receivedMessages = new LinkedList<>();
        messagesLedger = new LinkedList<>();
        missingMessages = new LinkedList<>();
        broadcastPeers = HashMultimap.create();
        backupPeers = new HashSet<>();
        loadEstimates = HashMultimap.create();
    }

    public void start(PeerSamplingService peerSamplingService, int epoch)
    {
        start(peerSamplingService, epoch, secondsToMillis(10), secondsToMillis(1), secondsToMillis(10), secondsToMillis(10));
    }

    static long secondsToMillis(int seconds)
    {
        return TimeUnit.MILLISECONDS.convert(seconds, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    public void start(PeerSamplingService peerSamplingService, int epoch,
                      long summaryStartMillis, long summaryDelayMillis,
                      long missingMessageStartMiils, long missingMessageDelayMiils)
    {
        if (executing)
            return;
        this.peerSamplingService = peerSamplingService;
        idGenerator = new GossipMessageId.IdGenerator(epoch);
        backupPeers.addAll(peerSamplingService.getPeers());
        executing = true;
        summarySender = scheduledTasks.scheduleWithFixedDelay(new SummarySender(), summaryStartMillis, summaryDelayMillis, TimeUnit.MILLISECONDS);
        missingMessagesResolver = scheduledTasks.scheduleWithFixedDelay(new MissingMessagesTimer(), missingMessageStartMiils, missingMessageDelayMiils, TimeUnit.MILLISECONDS);
    }

    /**
     * Shut down the broadcast behaviors, and clear out some data structures.
     * On process shutdown, this is not so interesting, but if temporarily stopping gossip (via nodetool, for example)
     * it will reset everything for starting up again.
     */
    public void shutdown()
    {
        if (!executing)
            return;
        executing = false;

        if (summarySender != null)
            summarySender.cancel(true);
        if (missingMessagesResolver != null)
            missingMessagesResolver.cancel(true);

        broadcastPeers.clear();
        backupPeers.clear();
        missingMessages.clear();
        receivedMessages.clear();
        messagesLedger.clear();
        loadEstimates.clear();
    }

    /**
     * Broadcast a client's message down through the spanning tree. If we detect that the number of first-level peers
     * is less than the fanout (probably because a node was shutdown or a partition occured or ...), select additional peers
     * from the {@link ThicketService#backupPeers} to ensure a full fanout.
     *
     * The actual broadcast is scheduled into the internal (single-threaded) executor service as the
     * act of broadcasting will need to reference internal state, which is not thread-safe.
     */
    public void broadcast(final Object payload, final BroadcastServiceClient client)
    {
        executor.execute(() -> performBroadcast(payload, client));
    }

    void performBroadcast(Object payload, BroadcastServiceClient client)
    {
        GossipMessageId messageId = idGenerator.generate();
        recordMessage(localAddress, messageId);

        // rebuild (or add to) the tree-root's peers if the size is less than the fanout
        Collection<InetAddress> peers = broadcastPeers.get(localAddress);
        int fanout = deriveFanout();
        if (peers.size() < fanout)
        {
            peers = selectRootBroadcastPeers(peers, backupPeers, fanout);
//            peers = selectRootBroadcastPeers(peers, peerSamplingService.getPeers(), fanout);
            broadcastPeers.putAll(localAddress, peers);
        }

        DataMessage msg = new DataMessage(localAddress, messageId, localAddress, payload, client.getClientName(), localLoadEstimate(broadcastPeers));
        for (InetAddress peer : peers)
            messageSender.send(peer, msg);
        broadcastedMessages.incrementAndGet();
    }

    /**
     * Select random peers from {@code peers} for the second-level nodes in a broadcast tree which is rooted at the current node.
     */
    @VisibleForTesting
    static Collection<InetAddress> selectRootBroadcastPeers(Collection<InetAddress> currentPeers, Collection<InetAddress> candidates, int maxActive)
    {
        LinkedList<InetAddress> newPeers = new LinkedList<>(currentPeers);
//        LinkedList<InetAddress> candidates = new LinkedList<>(peersPool);
//        Collections.shuffle(candidates);

        // TODO: if keeping this logic, clean it up
        Iterator<InetAddress> iter = candidates.iterator();
        while (newPeers.size() < maxActive && !iter.hasNext())
        {
            InetAddress peer = iter.next();
            if (!newPeers.contains(peer))
            {
                newPeers.add(peer);
                iter.remove();
            }
        }

        return newPeers;
    }

    /**
     * Calculate the fanout size, this is, the number of peers that will be broadcast to when the this node has a new
     * message is wants to send (acting as the tree-root). The fanout size imust be smaller than the {@link PeerSamplingService#getPeers()}
     * size, and a bit of headroom needs to be saved for healing broken branches, thus fanout must be smaller than the backupPeers size.
     * As the {@link PeerSamplingService#getPeers()} size already something on the order of the natural log of the node count of
     * the cluster, as long as we stay below that (or can adjust to that via PRUNE'ing) it should work well.
     */
    @VisibleForTesting
    int deriveFanout()
    {
        int size = peerSamplingService.getPeers().size();
        // NOTE: these values weren't scientifically calculated, but seemed reasonable based on comparisions with the thicket paper
        // and other broadcast tree gossip implementations

        // create a fully connected tree, it will prune itself quite quickly if need be
        if (size <= 2)
            return size;
        if (size < 5)
            return size - 1;

        // if you get this far, either the cluster has a large number of nodes in the local datacenter, many datacenters,
        // or a combination of the both.

        if (size < 10)
            return size - 3;

        return (int)Math.ceil(size / 2);
    }

    /**
     * Determine the maxLoad for forwarding DATA messages to peers.
     *
     * A careful reading of the thicket and hyparview papers will reveal that thicket's maxLoad value should be
     * "logarithmic with the number of nodes in the system"; while hyparview's  active view (which becomes thicket's
     * backupPeers set) is "log(n) + c", where n is the number of nodes in the system and c is a constant, usually 1
     * (according to the experiments in the paper). Thus, we would end up with a maxLoad value one less than the size
     * of the backupPeers size - which means there would always be an entry we could never make use of.
     *
     * Because of all this, maxLoad is simply the size of the backupPeers.
     */
    @VisibleForTesting
    int deriveMaxLoad()
    {
        return peerSamplingService.getPeers().size();
    }

    /**
     * Add the message id to the recently seen list.
     */
    private void recordMessage(InetAddress treeRoot, GossipMessageId messageId)
    {
        TimestampedMessageId msgId = new TimestampedMessageId(treeRoot, messageId, System.nanoTime() + MESSAGE_ID_RETENTION_TIME);
        receivedMessages.add(msgId);
    }

    public void receiveMessage(ThicketMessage message)
    {
        try
        {
            switch (message.getMessageType())
            {
                case DATA:      handleData((DataMessage)message); break;
                case SUMMARY:   handleSummary((SummaryMessage)message); break;
                case GRAFT:     handleGraft((GraftMessage)message); break;
                case PRUNE:     handlePrune((PruneMessage)message); break;
                default:
                    throw new IllegalArgumentException("unknown message type: " + message.getMessageType());
            }
        }
        catch (Exception e)
        {
            logger.error("Error processing broadcast message", e);
        }
    }

    void handleData(DataMessage message)
    {
        recordMessage(message.treeRoot, message.messageId);
        removeFromMissing(missingMessages, message.treeRoot, message.messageId);

        BroadcastServiceClient client = clients.get(message.client);
        if (client == null)
        {
            logger.warn("recieved broadcast message for unknown client component: " + message.client);
            return;
        }

        // should we also look at the message id, as well, in determining if the message is stale?
        if (client.receive(message.payload))
        {
            relayMessage(message);
        }
        else
        {
            logger.info(String.format("%s sending PRUNE to %s due to duplicate message", localAddress, message.sender));
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), Collections.singletonList(message.treeRoot), localLoadEstimate(broadcastPeers)));
        }
    }

    /**
     * Remove an entry from the missing messages collection (for a goven tree root). After removing, if there are no further
     * outstanding missing messages ids for the tree-root, remove it's entry from {@code missing}, as well.
     */
    @VisibleForTesting
    static void removeFromMissing(List<MissingMessges> missing, InetAddress treeRoot, GossipMessageId messageId)
    {
        // as each tree should only appear in the list of missing messages once, we can break after the first occurence.
        for (MissingMessges msgs : missing)
        {
            MissingSummary missingSummary = msgs.trees.get(treeRoot);
            if (missingSummary != null)
            {
                missingSummary.messages.remove(messageId);
                if (missingSummary.messages.isEmpty())
                    msgs.trees.remove(treeRoot);

                break;
            }
        }
    }

    /**
     * Send a received DATA message to peers on downstream branches, if any. The current node can be identified as a leaf in the
     * tree because it's broadcast peers list will only contain the peer that sent the DATA message to it.
     */
    void relayMessage(DataMessage message)
    {
        Collection<InetAddress> peers = broadcastPeers.get(message.sender);

        // check if we are already a leaf in this tree
        if (peers.size() == 1 && peers.iterator().next().equals(message.sender))
        {
            // TODO:JEB capture this data as a reportable metric, but for now log
//            logger.info(String.format("last delivery for %s, count = %s", message.messageId, message.hopCount));
            return;
        }

        if (peers.isEmpty())
        {
            if (isInterior(broadcastPeers))
            {
                peers = new LinkedList<>();
                peers.add(message.sender);
            }
            else
            {
                peers = selectBranchBroadcastPeers(message.sender, deriveFanout());
            }
        }
        broadcastPeers.putAll(message.treeRoot, peers);

        DataMessage msg = new DataMessage(localAddress, localLoadEstimate(broadcastPeers), message);
        peers.stream().filter(peer -> !peer.equals(message.sender) && !peer.equals(message.treeRoot)).forEach(peer -> messageSender.send(peer, msg));
    }

    /**
     * Check if the current node is interior to at least one tree in the cluster.
     */
    @VisibleForTesting
    boolean isInterior(Multimap<InetAddress, InetAddress> broadcastPeers)
    {
        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : broadcastPeers.asMap().entrySet())
        {
            // ignore the entry for the localAddress (this node)
            if (entry.getValue().size() > 1 && !entry.getKey().equals(localAddress))
                return true;
        }
        return false;
    }

    /**
     * Select random peers from {@code ThicketService#backupPeers} for the next-level nodes in a broadcast tree
     * which is not rooted at the current node.
     * Note: {@code ThicketService#backupPeers} will most likely be mutated as a result of this method.
     */
    @VisibleForTesting
    Collection<InetAddress> selectBranchBroadcastPeers(InetAddress upstreamPeer, int maxActive)
    {
        LinkedList<InetAddress> active = new LinkedList<>();
        active.add(upstreamPeer);

        // copy the peers into a LinkedList so we can shuffle and remove easily
        LinkedList<InetAddress> peersList = new LinkedList<>(backupPeers);
        Collections.shuffle(peersList);
        while (active.size() < maxActive && !peersList.isEmpty())
        {
            InetAddress peer = peersList.removeFirst();
            if (!active.contains(peer))
                active.add(peer);
        }

        backupPeers.removeAll(active);
        return active;
    }

    /**
     * Invoked periodically, this will send a SUMMARY message containing all received message IDs to peers in the backup set,
     * assumming, of course, we have any recently received messages. Further, if this node is at or above it's max load threshold,
     * it cannot help out other nodes with tree repair, and thus no SUMMARY message is sent out.
     *
     * In the Thicket paper, SUMMARY messages are sent to all nodes in the backup peers set. However, we treat the backup peers
     * differently from the paper (nodes are reused across tree-roots), and so we don't have a clean, easy to use set of peers.
     */
    void sendSummary()
    {
        if (!executing || receivedMessages.isEmpty())
            return;

        Collection<LoadEstimate> loadEstimates = localLoadEstimate(broadcastPeers);
        if (loadCount(loadEstimates) >= deriveMaxLoad())
            return;

        SummaryMessage message = new SummaryMessage(localAddress, idGenerator.generate(), convert(receivedMessages), loadEstimates);
        for (InetAddress peer : backupPeers)
            messageSender.send(peer, message);

        pruneMessageLedger(messagesLedger);
        messagesLedger.addAll(receivedMessages);
        receivedMessages.clear();
    }

    /**
     * Create a view of the load estimate for this node that can be sent to peers. The return value will not contain
     * entries for trees where this node is a leaf (as that does not contribute to the load factor of the node).
     */
    @VisibleForTesting
    static Collection<LoadEstimate> localLoadEstimate(Multimap<InetAddress, InetAddress> broadcastPeers)
    {
        List<LoadEstimate> estimates = new LinkedList<>();

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : broadcastPeers.asMap().entrySet())
        {
            if (entry.getValue().size() > 1)
            {
                // subtract one from the size because we include the upstream peer (the one that sends us the message)
                // in the list, so remove it from the count as we don't send messages to it in this tree
                int load = entry.getValue().size() - 1;
                estimates.add(new LoadEstimate(entry.getKey(), load));
            }
        }

        return estimates;
    }

    private static int loadCount(Collection<LoadEstimate> estimates)
    {
        int load = 0;
        for (LoadEstimate estimate : estimates)
            load += estimate.load;
        return load;
    }

    private Multimap<InetAddress, GossipMessageId> convert(List<TimestampedMessageId> receivedMessages)
    {
        Multimap<InetAddress, GossipMessageId> result = HashMultimap.create();
        for (TimestampedMessageId msgId : receivedMessages)
            result.put(msgId.treeRoot, msgId.messageId);
        return result;
    }

    /**
     * Remove expired entries from the {@code messagesLedger}.
     */
    void pruneMessageLedger(List<TimestampedMessageId> messagesLedger)
    {
        // perhaps we should bound the overall number of messages in the ledger, as well?

        // prune the message ledger - since this class is single threaded, we can assume the messages are in arrival timestamp order
        final long now = System.nanoTime();
        for (Iterator<TimestampedMessageId> iter = messagesLedger.iterator(); iter.hasNext(); )
        {
            if (iter.next().expirationTime >= now)
                break;
            iter.remove();
        }
    }

    /**
     * Process an incoming SUMMARY message from a peer. The goal here is to ensure all trees are healthy (no broken branches,
     * entire tree is spanned), *not* data convergence. Data convergence can be piggy-backed here for convenience,
     * or via anti-entropy, but that's not the primary purpose of the SUMMARY flow.
     */
    void handleSummary(SummaryMessage msg)
    {
        // do we want to send a response back to the SUMMARY sender? This might be a nice optimization to prevent
        // peers from sending a bunch of SUMMARY messages to us

        recordLoadEstimates(loadEstimates, msg.sender, msg.estimates);

        Multimap<InetAddress, GossipMessageId> reportedMissing = filterMissingMessages(msg.receivedMessages, mergeSeenMessages());

        if (reportedMissing.isEmpty())
            return;

        addToMissingMessages(msg.sender, missingMessages, reportedMissing);
    }

    private static void recordLoadEstimates(Multimap<InetAddress, LoadEstimate> localEstimates, InetAddress peer, Collection<LoadEstimate> estimates)
    {
        localEstimates.removeAll(peer);
        localEstimates.putAll(peer, estimates);
    }

    private Multimap<InetAddress, GossipMessageId> mergeSeenMessages()
    {
        Multimap<InetAddress, GossipMessageId> seenMessagesPerTreeRoot = HashMultimap.create();

        for (TimestampedMessageId msgId : receivedMessages)
            seenMessagesPerTreeRoot.put(msgId.treeRoot, msgId.messageId);

        for (TimestampedMessageId msgId : messagesLedger)
            seenMessagesPerTreeRoot.put(msgId.treeRoot, msgId.messageId);

        return seenMessagesPerTreeRoot;
    }

    /**
     * Filter out messages the local node has already recieved. Mutates the {@code summary} parameter in place to avoid allocating
     * a new Multimap.
     */
    @VisibleForTesting
    Multimap<InetAddress, GossipMessageId> filterMissingMessages(Multimap<InetAddress, GossipMessageId> summary, Multimap<InetAddress, GossipMessageId> seenMessagesPerTreeRoot)
    {
        Multimap<InetAddress, GossipMessageId> filtered = HashMultimap.create();

        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : summary.asMap().entrySet())
        {
            // probably reasonable to assume we've seen our own messages
            if (entry.getKey().equals(localAddress))
                continue;

            List<GossipMessageId> ids = new ArrayList<>(entry.getValue());
            ids.removeAll(seenMessagesPerTreeRoot.get(entry.getKey()));

            if (!ids.isEmpty())
                filtered.putAll(entry.getKey(), ids);
        }

        return filtered;
    }

    /**
     * Add missing message ids to existing {@code missingMessages} instances (indexed by tree-root), and if any
     * {@code reportedMissing} entries remain, add new structs to the list.
     */
    @VisibleForTesting
    static void addToMissingMessages(InetAddress sender, List<MissingMessges> missingMessages, Multimap<InetAddress, GossipMessageId> reportedMissing)
    {
        for (MissingMessges missingMessges : missingMessages)
        {
            for (Map.Entry<InetAddress, MissingSummary> entry : missingMessges.trees.entrySet())
            {
                Collection<GossipMessageId> ids = reportedMissing.removeAll(entry.getKey());
                if (ids != null && !ids.isEmpty())
                    entry.getValue().add(sender, ids);
            }
        }

        MissingMessges msgs = new MissingMessges(System.nanoTime() + MESSAGE_ID_RETENTION_TIME);
        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : reportedMissing.asMap().entrySet())
        {
            MissingSummary missingSummary = new MissingSummary();
            missingSummary.add(sender, entry.getValue());
            msgs.trees.put(entry.getKey(), missingSummary);
        }
        missingMessages.add(msgs);
    }

    /**
     * A periodically invoked method that examines the missing messages set that we've recieved vai SUMMARY messages, and if
     * those message ids are still outstanding, attempts to heal broken branches by GRAFTing peers into the tree.
     */
    void checkMissingMessages()
    {
        if (!executing || missingMessages.isEmpty())
            return;

        // map of tree-root to GRAFT candidates
        Multimap<InetAddress, InetAddress> graftCandidates = discoverGraftCandidates(missingMessages);
        if (graftCandidates == null)
            return;

        int maxLoad = deriveMaxLoad();
        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftCandidates.asMap().entrySet())
        {
            Optional<InetAddress> target = detetmineBestCandidate(loadEstimates, entry.getValue(), maxLoad);
            if (target.isPresent())
                messageSender.send(target.get(), new GraftMessage(localAddress, idGenerator.generate(), entry.getValue(), localLoadEstimate(broadcastPeers)));
        }
    }

    /**
     * For each tree-root with missing messages, find potential peers to GRAFT onto.
     * @return a multimap of tree-root to potential GRAFT peers
     */
    @VisibleForTesting
    static Multimap<InetAddress, InetAddress> discoverGraftCandidates(List<MissingMessges> missing)
    {
        // tree-root -> graft peers
        Multimap<InetAddress, InetAddress> graftCandidates = null;

        long now = System.nanoTime();
        for (MissingMessges msgs : missing)
        {
            // as MissingMessages instances are in order (by timestamp) in the List, if we hit an instance
            // with a higher timerstamp, we 're iterating
            if (msgs.evaluationTimestamp > now)
                break;
            for (Map.Entry<InetAddress, MissingSummary> entry : msgs.trees.entrySet())
            {
                if (entry.getValue().messages.isEmpty() || entry.getValue().messages.isEmpty())
                    continue;

                if (graftCandidates == null)
                    graftCandidates = HashMultimap.create();
                graftCandidates.putAll(entry.getKey(), entry.getValue().peers);
            }
        }

        return graftCandidates;
    }

    @VisibleForTesting
    static Optional<InetAddress> detetmineBestCandidate(Multimap<InetAddress, LoadEstimate> loadEstimates, Collection<InetAddress> treeCandidates, int maxLoad)
    {
        LinkedList<InetAddress> filtered = new LinkedList<>();
        for (InetAddress candidate : treeCandidates)
        {
            if (!isOverMaxLoad(loadEstimates.get(candidate), maxLoad))
                filtered.add(candidate);
        }

        if (!filtered.isEmpty())
        {
            Collections.shuffle(filtered);
            return Optional.of(filtered.getFirst());
        }
        return Optional.empty();
    }

    /**
     * Determine if a node is at over the maxLoad allowed by inspecting the {@code loadEstimates} collection.
     * In addition to not being over the maxLoad, the node should be interior to a limited number of trees.
     * {@code loadEstimates} should not include entries for tree where the node is a leaf.
     */
    @VisibleForTesting
    static boolean isOverMaxLoad(Collection<LoadEstimate> loadEstimates, int maxLoad)
    {
        // not sure what is the right value here for loadEst.size() to be less than (meaning, how many trees
        // should the node be interior to, at the max). The thicket paper is hand-wavy about the 'real' max count when it
        // comes to interiority and healing trees, we'll make our best guess as practicioners.
        return loadCount(loadEstimates) >= maxLoad || loadEstimates.size() >= 3;
    }

    /**
     * Process an incoming GRAFT message. If we have not exceeded the maxLoad limit on this node, go ahead and accept
     * the GRAFT request and heal the tree by adding the sender to the broadcastPeers for the given tree-roots.
     * Else if we are over the limit, send a PRUNE message back to the sender (maybe it will have better luck next time
     * finding a peer to heal it's branch).
     */
    void handleGraft(GraftMessage message)
    {
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

         // should we calculate this per-tree, or make life simpler and just take the whole bunch and potentially be interior
        // to more trees?
        if (!isOverMaxLoad(localLoadEstimate(broadcastPeers), deriveMaxLoad()))
        {
            for (InetAddress treeRoot : message.treeRoots)
            {
                Collection<InetAddress> peers = broadcastPeers.get(treeRoot);
                peers.add(message.sender);
            }
        }
        else
        {
//            logger.info(String.format("%s sending PRUNE to %s due denied GRAFT", localAddress, message.sender));
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), message.treeRoots, localLoadEstimate(broadcastPeers)));
        }
    }

    /**
     * When a PRUNE message is received, remove it from the active peers for the tree-roots in the message. Then,
     * add it to the backup peers.
     */
    void handlePrune(PruneMessage message)
    {
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

        for (InetAddress treeRoot : message.treeRoots)
        {
            Collection<InetAddress> branchPeers = broadcastPeers.get(treeRoot);
            branchPeers.remove(message.sender);
        }

        backupPeers.add(message.sender);
    }

    public void register(BroadcastServiceClient client)
    {
        clients.put(client.getClientName(), client);
    }

    public void neighborUp(InetAddress peer, String datacenter)
    {
        backupPeers.add(peer);
    }

    public void neighborDown(InetAddress peer, String datacenter)
    {
        broadcastPeers.removeAll(peer);
        for (InetAddress treeRoot : broadcastPeers.keySet())
            broadcastPeers.remove(treeRoot, peer);

        backupPeers.remove(peer);

        for (MissingMessges missingMessage : missingMessages)
        {
            if (missingMessage.trees.remove(peer) != null)
                break;
        }
        loadEstimates.removeAll(peer);
    }

    private class SummarySender implements Runnable
    {
        public void run()
        {
            executor.execute(ThicketService.this::sendSummary);
        }
    }

    private class MissingMessagesTimer implements Runnable
    {
        public void run()
        {
            executor.execute(ThicketService.this::checkMissingMessages);
        }
    }

    @VisibleForTesting
    InetAddress getLocalAddress()
    {
        return localAddress;
    }

    @VisibleForTesting
    Collection<InetAddress> getBackupPeers()
    {
        return backupPeers;
    }

    @VisibleForTesting
    Multimap<InetAddress, InetAddress> getBroadcastPeers()
    {
        return broadcastPeers;
    }

    @VisibleForTesting
    public int getBroadcastedMessageCount()
    {
        return broadcastedMessages.get();
    }

    /**
     * A simple struct to capture metadata about recently received messages from peers.
     */
    static class TimestampedMessageId
    {
        final InetAddress treeRoot;
        final GossipMessageId messageId;
        final long expirationTime;

        private TimestampedMessageId(InetAddress treeRoot, GossipMessageId messageId, long expirationTime)
        {
            this.treeRoot = treeRoot;
            this.messageId = messageId;
            this.expirationTime = expirationTime;
        }
    }

    /**
     * A struct which contains the missing message ids, per tree-root. All missing message ids for a given tree root are captured
     * together, regardless of when a SUMMARY message arrived, and before the timer expires. This means if we get one SUMAMRY message
     * with some message ids that we don't have at time t1, a timer is set to go off at time t10, any SUMMARY message received
     * before t10 with further message ids we don't have for the given tree-root are added to the same instance.
     */
    static class MissingMessges
    {
        /**
         * Mapping of tree-root to the collection of missing message summaries.
         */
        final Map<InetAddress, MissingSummary> trees;

        /**
         * Timestamp after which this data should be evaluated.
         */
        final long evaluationTimestamp;

        MissingMessges(long evaluationTimestamp)
        {
            this.evaluationTimestamp = evaluationTimestamp;
            trees = new HashMap<>();
        }
    }

    /**
     * A struct to hold the missing message ids and the peers who informed us of those. Ultimately, it doesn't
     * matter so much which peer reported ids, as any of them still received informatation before we did.
     */
    static class MissingSummary
    {
        /**
         * Total set of message ids (for a given tree) from all peers that sent SUMMARY messages.
         */
        final Set<GossipMessageId> messages;

        /**
         * Collection of peers who sent a SUMMARY message.
         */
        final Set<InetAddress> peers;

        MissingSummary()
        {
            messages = new HashSet<>();
            peers = new HashSet<>();
        }

        void add(InetAddress peer, Collection<GossipMessageId> ids)
        {
            peers.add(peer);
            messages.addAll(ids);
        }
    }
}
