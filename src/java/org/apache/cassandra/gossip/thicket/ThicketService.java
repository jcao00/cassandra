package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
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
     * Mapping of a message originatorig's address to the downstream branch peers.
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
        backupPeers = new LinkedList<>();
        loadEstimates = HashMultimap.create();
    }

    public void start(PeerSamplingService peerSamplingService, int epoch)
    {
        if (executing)
            return;
        this.peerSamplingService = peerSamplingService;
        idGenerator = new GossipMessageId.IdGenerator(epoch);
        backupPeers.addAll(peerSamplingService.getPeers());
        executing = true;
        summarySender = scheduledTasks.scheduleWithFixedDelay(new SummarySender(), 10, 1, TimeUnit.SECONDS);
        missingMessagesResolver = scheduledTasks.scheduleWithFixedDelay(new MissingMessagesTimer(), 10, 1, TimeUnit.SECONDS);
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
        summarySender.cancel(true);
        missingMessagesResolver.cancel(true);

        broadcastPeers.clear();
        backupPeers.clear();
        missingMessages.clear();
        receivedMessages.clear();
        messagesLedger.clear();
        loadEstimates.clear();
    }

    public void broadcast(Object payload, BroadcastServiceClient client)
    {
        GossipMessageId messageId = idGenerator.generate();
        recordMessage(localAddress, messageId);

        // TODO:JEB might want to consider rebuilding the root peers is the size is less than the fanout
        Collection<InetAddress> peers = broadcastPeers.get(localAddress);
        if (peers.isEmpty())
        {
            peers = selectRootBroadcastPeers(peerSamplingService.getPeers(), deriveFanout());
            broadcastPeers.putAll(localAddress, peers);
        }

        DataMessage msg = new DataMessage(localAddress, messageId, localAddress, payload, client.getClientName());
        for (InetAddress peer : peers)
            messageSender.send(peer, msg);
    }

    /**
     * Select random peers from {@code peers} for the second-level nodes in a broadcast tree which is rooted at the current node.
     */
    @VisibleForTesting
    Collection<InetAddress> selectRootBroadcastPeers(Collection<InetAddress> peers, int maxActive)
    {
        LinkedList<InetAddress> candidates = new LinkedList<>(peers);
        Collections.shuffle(candidates);

        while (candidates.size() > maxActive)
            candidates.removeFirst();

        return candidates;
    }

    /**
     * Calculate the fanout size. There's a bit of bleed through of abstractions here. We assume HyParView as the underlying
     * PeerSamplingService, and it's fanout size is, more or less, log(n) + c, where n is the number of nodes in the cluster
     * and c is a constant, almost always 1. Since Thicket works by using log(n) for calculating it's own active peers size,
     * we just need to subtract the constant c to derive the correct size. Of course, we're completely fudging around the way
     * that the HyParView implementation deals with multiple datacenters, but that should not affect the basic notion here:
     * subtract the constant from HyParView's getPeers() size to get the Thicket active peers size.
     */
    @VisibleForTesting
    int deriveFanout()
    {
        // TODO:JEB fix me
        int size = backupPeers.size();

        return size == 0 ? 0 : size == 1 ? 1 : size - 1;
    }

    @VisibleForTesting
    int deriveMaxLoad()
    {
        // TODO:JEB fix me - this is totally wrong
        int size = backupPeers.size();

        return size == 0 ? 0 : size == 1 ? 1 : size - 1;
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
            logger.info("recieved broadcast message for unknown client component: " + message.client);
            return;
        }

        // TODO:JEB should we also look at the message id, as well, in determining if the message is stale?
        if (client.receive(message.payload))
            relayMessage(message);
        else
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), Collections.singletonList(message.treeRoot), loadEstimates.get(localAddress)));
    }

    /**
     * Remove an entry from the missing messages collection (for a goven tree root). After removing, if there are no further
     * outstanding missing messages ids for the tree-root, remove it's entry from {@code missing}, as well.
     */
    @VisibleForTesting
    void removeFromMissing(List<MissingMessges> missing, InetAddress treeRoot, GossipMessageId messageId)
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
     * Send a received message to peers on downstream branches, if any.
     */
    void relayMessage(DataMessage message)
    {
        //TODO:JEB test me
        Collection<InetAddress> peers = broadcastPeers.get(message.sender);
        if (peers.isEmpty())
        {
            // TODO:JEB is this really the correct metric?? just identifying if we're interior in a tree? what about if
            // we've still got capacity under the maxLoad?
            if (isInterior(broadcastPeers))
            {
                peers = new LinkedList<>();
                peers.add(message.sender);
            }
            else
            {
                peers = selectBranchBroadcastPeers(message.sender, deriveFanout());
                broadcastPeers.putAll(localAddress, peers);
            }
        }

        GossipMessageId messageId = idGenerator.generate();
        DataMessage msg = new DataMessage(localAddress, messageId, message.treeRoot, message.payload, message.client);
        peers.stream().filter(peer -> !peer.equals(message.sender)).forEach(peer -> messageSender.send(peer, msg));
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
        //TODO:JEB test me
        LinkedList<InetAddress> active = new LinkedList<>();
        active.add(upstreamPeer);
        maxActive--;

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
     * INvoked periodically, this will send a SUMMARY message containing all received message IDs to peers in the backup set,
     * assumming, of course, we have any recently received messages. Further, if this node is at or above it's max load threshold,
     * it cannot help out other nodes with tree repair, and thus no SUMMARY message is sent out.
     *
     * In the Thicket paper, SUMMARY messages are sent to all nodes in the backup peers set. However, we treat the backup peers
     * differently from the paper (nodes are reused across tree-roots), and so we don't have a clean, easy to use set of peers.
     */
    void sendSummary()
    {
        //TODO:JEB test me

        if (!executing || receivedMessages.isEmpty() || isOverMaxLoad() || backupPeers.isEmpty())
            return;

        SummaryMessage message = new SummaryMessage(localAddress, idGenerator.generate(), convert(receivedMessages), loadEstimates.get(localAddress));
        for (InetAddress peer : backupPeers)
            messageSender.send(peer, message);

        pruneMessageLedger(messagesLedger);
        messagesLedger.addAll(receivedMessages);
        receivedMessages.clear();
    }

    @VisibleForTesting
    boolean isOverMaxLoad()
    {
        // TODO:JEB test me
        // TODO:JEB base on loadEst
        return true;
    }

    private Multimap<InetAddress, GossipMessageId> convert(List<TimestampedMessageId> receivedMessages)
    {
        Multimap<InetAddress, GossipMessageId> result = HashMultimap.create();
        for (TimestampedMessageId msgId : receivedMessages)
            result.put(msgId.treeRoot, msgId.messageId);
        return result;
    }

    void pruneMessageLedger(List<TimestampedMessageId> messagesLedger)
    {
        // TODO:JEB test me

        // TODO:JEB perhaps we should bound the overall number of messages in the ledger, as well
        //prune the message ledger - since this class is single threaded, we can assume the messages are in arrival timestamp order
        final long now = System.nanoTime();
        for (Iterator<TimestampedMessageId> iter = messagesLedger.iterator(); iter.hasNext(); )
        {
            if (iter.next().expirationTime >= now)
                break;
            iter.remove();
        }
    }

    private void recordLoadEstimates(Multimap<InetAddress, LoadEstimate> localEstimates, InetAddress peer, Collection<LoadEstimate> estimates)
    {
        localEstimates.removeAll(peer);
        localEstimates.putAll(peer, estimates);
    }

    /**
     * Process an incoming SUMMARY message from a peer. The goal here is to ensure all trees are healthy (no broken branches,
     * entire tree is spanned), *not* data convergence. Data convergence can be piggy-backed here for convenience,
     * or via anti-entropy, but that's not the primary purpose of the SUMMARY flow.
     */
    void handleSummary(SummaryMessage msg)
    {
        // TODO:JEB test me

        // TODO:JEB do we want to send a response back to the SUMMARY sender?

        recordLoadEstimates(loadEstimates, msg.sender, msg.estimates);

        Multimap<InetAddress, GossipMessageId> reportedMissing = msg.receivedMessages;
        filterMissingMessages(reportedMissing, mergeSeenMessages());

        if (reportedMissing.isEmpty())
            return;

        addToMissingMessages(msg.sender, missingMessages, reportedMissing);
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
    void filterMissingMessages(Multimap<InetAddress, GossipMessageId> summary, Multimap<InetAddress, GossipMessageId> seenMessagesPerTreeRoot)
    {
        // TODO:JEB test me (might already be done)
        // probably reasonable to assume we've seen our own messages
        summary.removeAll(localAddress);

        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : summary.asMap().entrySet())
        {
            Collection<GossipMessageId> ids = entry.getValue();
            ids.removeAll(seenMessagesPerTreeRoot.get(entry.getKey()));

            if (ids.isEmpty())
                summary.removeAll(entry.getKey());
        }
    }

    /**
     * Add missing message ids to existing {@code missingMessages} instances (indexed by tree-root), and if any
     * {@code reportedMissing} entries remain, add new structs to the list.
     */
    @VisibleForTesting
    void addToMissingMessages(InetAddress sender, List<MissingMessges> missingMessages, Multimap<InetAddress, GossipMessageId> reportedMissing)
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
        //TODO:JEB test me
        if (!executing || missingMessages.isEmpty())
            return;

        // map of tree-root to GRAFT candidates
        Multimap<InetAddress, InetAddress> graftCandidates = discoverGraftCandidates(missingMessages);
        if (graftCandidates == null)
            return;

        // map of GRAFT targets to the tree-roots that are being healed (by the GRAFT target)
        Multimap<InetAddress, InetAddress> graftTargets = calculateGraftTragets(loadEstimates, graftCandidates, deriveMaxLoad());

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftTargets.asMap().entrySet())
            messageSender.send(entry.getKey(), new GraftMessage(localAddress, idGenerator.generate(), entry.getValue(), loadEstimates.get(localAddress)));
    }

    /**
     * Find potential peers to GRAFT for each tree-root for which there are missing messages.
     */
    Multimap<InetAddress, InetAddress> discoverGraftCandidates(List<MissingMessges> missing)
    {
        //TODO:JEB test me

        // map of peer address to which we want to add current node for the given tree roots.
        // creating this structure to optimize the GRAFT message count we send
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

    private Multimap<InetAddress, InetAddress> calculateGraftTragets(Multimap<InetAddress, LoadEstimate> loadEstimates,
                                                                     Multimap<InetAddress, InetAddress> graftCandidates,
                                                                     int maxLoad)
    {
        // TODO:JEB test me
        Multimap<InetAddress, InetAddress> targetToTrees = HashMultimap.create();

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftCandidates.asMap().entrySet())
        {
            Collection<InetAddress> treeCandidates = entry.getValue();
            // err, this case really shouldn't happen, but let's be safe
            if (treeCandidates.size() == 0)
                continue;
            if (treeCandidates.size() == 1)
            {
                targetToTrees.put(treeCandidates.iterator().next(), entry.getKey());
                continue;
            }

            // select peer with lowest loadEst
            Optional<InetAddress> target = detetmineBestCandidate(loadEstimates, treeCandidates, maxLoad);
            if (target.isPresent())
                targetToTrees.put(target.get(), entry.getKey());
        }

        return targetToTrees;
    }

    private Optional<InetAddress> detetmineBestCandidate(Multimap<InetAddress, LoadEstimate> loadEstimates, Collection<InetAddress> treeCandidates, int maxLoad)
    {
        // TODO:JEB test me

        LinkedList<InetAddress> filtered = new LinkedList<>();
        for (InetAddress candidate : treeCandidates)
        {
            Collection<LoadEstimate> loadEst = loadEstimates.get(candidate);
            // either we have no data on the target, or it really is not in any tree (as an interior node)
            if (loadEst.isEmpty())
                filtered.add(candidate);

            // if the node is interior in n number of trees already, pass on it
            int load = loadEstimateSum(loadEst);

            // TODO:JEB what is the right value here for loadEst.size() (which indicates the number of trees where node is interior)?
            // is it 2?  3?  ?????
            if (load < maxLoad && loadEst.size() < 3)
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
     * Sum up the number of peers to which a node would be forwarding DATA broadcasts. If the loadEstimate for a given
     * tree is equal to 1, then the node if just a leaf in the tree (and does not count toward the sum being calculated).
     *
     */
    private int loadEstimateSum(Collection<LoadEstimate> loadEstimates)
    {
        // TODO:JEB test me
        int sum = 0;
        for (LoadEstimate loadEstimate : loadEstimates)
        {
            if (loadEstimate.load > 1)
                sum += loadEstimate.load;
        }
        return sum;
    }

    void handleGraft(GraftMessage message)
    {
        // TODO:JEB test me
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

        if (!isOverMaxLoad())
        {
            for (InetAddress treeRoot : message.treeRoots)
            {
                Collection<InetAddress> peers = broadcastPeers.get(treeRoot);
                peers.add(message.sender);
            }
        }
        else
        {
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), message.treeRoots, loadEstimates.get(localAddress)));
        }
    }

    /**
     * When a PRUNE message is received, remove it from the active peers for the tree-root in the message. Then,
     * add it to the backup peers.
     */
    void handlePrune(PruneMessage message)
    {
        // TODO:JEB test me

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
        // TODO:JEB test me
        broadcastPeers.removeAll(peer);
        for (Iterator<InetAddress> iter = broadcastPeers.values().iterator(); iter.hasNext(); )
        {
            if (iter.next().equals(peer))
                iter.remove();
        }
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

    /**
     * A simple struct to capture metadata about recently received messages from peers.
     */
    class TimestampedMessageId
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
