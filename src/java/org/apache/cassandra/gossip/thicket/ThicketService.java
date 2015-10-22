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

// TODO:JEB add more documentation
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
     * The maximum number of message ids to retain (for SUMMARY purposes) per rooted spanning tree.
     */
    private static final int MESSAGE_ID_RETAIN_COUNT = 8;

    private static final long MESSAGE_ID_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(40, TimeUnit.SECONDS);

    /**
     * The braodcast address of the local node. We memoize it here to avoid a hard dependency on FBUtilities.
     */
    private final InetAddress localAddress;

    @VisibleForTesting
    final MessageSender messageSender;

    private PeerSamplingService peerSamplingService;

    /**
     * An executor pool to ensure all (mutation) behaviors are executed in a single thread of execution.
     */
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledTasks;
    private GossipMessageId.IdGenerator idGenerator;

    private boolean executing;

    /**
     * Mapping of a message originatorig's address to the downstream branch peers, both active and passive.
     *
     * "for each tree T rooted at R, here is set the downstream peers P"
     */
    private final Map<InetAddress, BroadcastPeers> broadcastPeers;

    /**
     * A common set of backup peers to be used in determining downstream broadcast peers.
     */
    private final Collection<InetAddress> backupPeers;

    private final Map<String, BroadcastServiceClient> clients;

    private ScheduledFuture<?> summarySender;
    private ScheduledFuture<?> missingMessagesResolver;

    /**
     * Collection of the most recently seen message ids. Entries will be published in SUMMARY messages, then moved to the recently
     * seen collection, and this list will be cleared for next batch of incoming messages.
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
     * (In the thicket paper, this data is referred to as the 'announcements' set.). Missing messages are grouped by the
     * tree root to which the messages belong (simplified, each message sender is the root of it's own tree), and because
     * branching is unique per-tree, we need to identify uniquely which tree is broken/partitioned/unhappy.
     */
    private final List<MissingMessges> missingMessages;

    /**
     * For each each peer, record it's {@link LoadEstimate} - the count of peers, per tree, that the node forwards broadcasted messages to.
     * This value is used when GRAFTing branches back onto a broken tree.
     */
    private final Multimap<InetAddress, LoadEstimate> loadEstimates;

    /**
     * Timestamp at which current thicket session started. This is necessary to avoid unnecessary tree thrashing (via GRAFT/PRUNE messages)
     * due to the arrival SUMMARYs that contain message ids tha never could have been received.
     */
    private long startTimeNanos;

    public ThicketService(InetAddress localAddress, MessageSender messageSender, ExecutorService executor, ScheduledExecutorService scheduledTasks)
    {
        this.localAddress = localAddress;
        this.messageSender = messageSender;
        this.executor = executor;
        this.scheduledTasks = scheduledTasks;

        clients = new HashMap<>();
        receivedMessages = new LinkedList<>();
        messagesLedger = new LinkedList<>();
        missingMessages = new LinkedList<>();
        broadcastPeers = new HashMap();
        backupPeers = new LinkedList<>();
        loadEstimates = HashMultimap.create();
    }

    public void start(PeerSamplingService peerSamplingService, int epoch)
    {
        start(peerSamplingService, epoch, System.nanoTime());
    }

    @VisibleForTesting
    void start(PeerSamplingService peerSamplingService, int epoch, long startTimeNanos)
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

    public void shutdown()
    {
        if (!executing)
            return;

        executing = false;
        summarySender.cancel(true);
        missingMessagesResolver.cancel(true);

        // clear out some data structures.
        // on process shutdown this is not so interesting, but if temporarily stopping gossip it will reset everything for starting up again
        broadcastPeers.clear();
        backupPeers.clear();
        missingMessages.clear();
        receivedMessages.clear();
        messagesLedger.clear();
    }

    public void broadcast(Object payload, BroadcastServiceClient client)
    {
        GossipMessageId messageId = idGenerator.generate();
        recordMessage(localAddress, messageId);

        BroadcastPeers peers = broadcastPeers.get(localAddress);
        if (peers == null || peers.activePeers.isEmpty())
        {
            peers = selectBroadcastPeers(backupPeers, Optional.empty(), deriveFanout());
            broadcastPeers.put(localAddress, peers);
        }

        DataMessage msg = new DataMessage(localAddress, messageId, localAddress, payload, client.getClientName());
        for (InetAddress peer : peers.activePeers)
            messageSender.send(peer, msg);
    }

    /**
     * Create a set of active and a set of backup peers from the provided collection. The size of the active set is bounded
     * by the {@code maxActive} parameter.
     */
    @VisibleForTesting
    BroadcastPeers selectBroadcastPeers(Collection<InetAddress> peers, Optional<InetAddress> include, int maxActive)
    {
        LinkedList<InetAddress> backup = new LinkedList<>(peers);
        LinkedList<InetAddress> active = new LinkedList<>();

        if (include.isPresent())
        {
            active.add(include.get());
            backup.remove(include.get());
            maxActive--;
        }

        // TODO:JEB when building a branch (not the tree root), need to check load of peer

        Collections.shuffle(backup);
        while (active.size() < maxActive && !backup.isEmpty())
            active.add(backup.removeFirst());

        return new BroadcastPeers(active, backup);
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
        //TODO:JEB test me
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
        {
            GossipMessageId messageId = idGenerator.generate();
            BroadcastPeers peers = broadcastPeers.get(message.sender);
            if (peers == null || peers.activePeers.isEmpty())
            {
                peers = selectBroadcastPeers(backupPeers, Optional.of(message.sender), deriveFanout());
                broadcastPeers.put(localAddress, peers);
            }

            DataMessage msg = new DataMessage(localAddress, messageId, message.treeRoot, message.payload, client.getClientName());
            for (InetAddress peer : peers.activePeers)
                messageSender.send(peer, msg);
        }
        else
        {
            // TODO:JEB we've seen the messageId already
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), Collections.singletonList(message.treeRoot), loadEstimates.get(localAddress)));
        }
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
     * Process an incoming SUMMARY message from a peer. The goal here is to ensure the tree is healthy (no broken branches,
     * entire tree is spanned), *not* data convergence. Data convergence can be piggy-backed here for convenience,
     * or via anti-entropy, but that's not the primary purpose of the SUMMARY flow.
     *
     * Also, if the current thicket session has just started (either due to process launch or enabling gossip via nodetool)
     * there's little to no chance we've seen any message ids that might be contained in the SUMMARY. To alleviate
     * any tree thrashing that might occur due to subsequent GRAFT/PRUNE actions, just ignore any SUMMARY messages for a brief
     * time after session start so we can catch up on the thicket traffic.
     */
    void handleSummary(SummaryMessage msg)
    {
        // TODO:JEB do we want to send a response back to the SUMMARY sender?

        // might not need a window as large as MESSAGE_ID_RETENTION_TIME, but it seems reasonable enough
        if (startTimeNanos + MESSAGE_ID_RETENTION_TIME > System.nanoTime())
            return;

        recordLoadEstimates(loadEstimates, msg.sender, msg.estimates);

        Multimap<InetAddress, GossipMessageId> reportedMissing = msg.receivedMessages;
        filterMissingMessages(reportedMissing, mergeSeenMessages());

        if (reportedMissing.isEmpty())
            return;

        addToMissingMessages(msg.sender, missingMessages, reportedMissing);
    }

    private void recordLoadEstimates(Multimap<InetAddress, LoadEstimate> localEstimates, InetAddress peer, Collection<LoadEstimate> estimates)
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
     * Filter out messages the local node has already recieved.
     */
    @VisibleForTesting
    void filterMissingMessages(Multimap<InetAddress, GossipMessageId> summary, Multimap<InetAddress, GossipMessageId> seenMessagesPerTreeRoot)
    {
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

    void handleGraft(GraftMessage message)
    {
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

        if (shouldAcceptGraft(message))
            applyGraft(broadcastPeers, message.treeRoots, message.sender);
        else
            messageSender.send(message.sender, new PruneMessage(localAddress, idGenerator.generate(), message.treeRoots, loadEstimates.get(localAddress)));
    }

    @VisibleForTesting
    boolean shouldAcceptGraft(GraftMessage message)
    {
        // TODO:JEB test me
        // TODO:JEB base on loadEst
        return true;
    }

    @VisibleForTesting
    void applyGraft(Map<InetAddress, BroadcastPeers> existingPeers, Collection<InetAddress> treeRoots, InetAddress sender)
    {
        for (InetAddress treeRoot : treeRoots)
        {
            BroadcastPeers peers = existingPeers.get(treeRoot);
            if (peers == null || peers.activePeers.isEmpty())
            {
                // It's highly unlikely we don't have an entry for the tree root (because we actaully told the peer
                // about missing messages *from* that tree root), but just to be safe recreate the peers set
                existingPeers.put(treeRoot, selectBroadcastPeers(backupPeers, Optional.of(sender), deriveFanout()));
            }
            else
            {
                peers.addToActive(sender);
            }
        }
    }

    /**
     * When a PRUNE message is received, remove it from the active peers for the tree-root in the message. Then,
     * add it to the backup peers.
     */
    void handlePrune(PruneMessage message)
    {
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

        for (InetAddress treeRoot : message.treeRoots)
        {
            BroadcastPeers peers = broadcastPeers.get(treeRoot);
            if (peers == null)
                continue;
            peers.moveToBackup(message.sender);
        }
    }

    /**
     * Send a SUMMARY message containing all received message IDs to all peers in the backup set, assumming, of course, we have any
     * recently received messages. Further, if this node is at or above it's max load threshold, it cannot help out other nodes
     * with tree repair, and thus no SUMMARY message is sent out.
     */
    void sendSummary()
    {
        //TODO:JEB test me

        if (!executing || receivedMessages.isEmpty())
            return;

        // TODO:JEB do not send SUMMARY if we're over the maxLoad threshold

        // TODO:JEB this function needs a refresher - since we're not keeping a 'pure' notion of backup peers (like the paper), we should
        // be smart about which peers we send a SUMMARY to (to not overwhelm peers)

        SummaryMessage message = new SummaryMessage(localAddress, idGenerator.generate(), convert(receivedMessages), loadEstimates.get(localAddress));
        for (InetAddress peer : backupPeers)
            messageSender.send(peer, message);

        pruneMessageLedger(messagesLedger);
        messagesLedger.addAll(receivedMessages);
        receivedMessages.clear();
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
        //TODO:JEB test me

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
        broadcastPeers.remove(peer);
        for (BroadcastPeers peers : broadcastPeers.values())
            peers.remove(peer);
        backupPeers.remove(peer);
    }

    void checkMissingMessages()
    {
        //TODO:JEB test me
        if (missingMessages.isEmpty())
            return;

        Multimap<InetAddress, InetAddress> graftTargets = discoverGraftTargets(missingMessages);
        if (graftTargets == null)
            return;

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftTargets.asMap().entrySet())
            messageSender.send(entry.getKey(), new GraftMessage(localAddress, idGenerator.generate(), entry.getValue(), loadEstimates.get(localAddress)));
    }

    Multimap<InetAddress, InetAddress> discoverGraftTargets(List<MissingMessges> missing)
    {
        //TODO:JEB test me

        // target address {key} to which we want to add current node for the given tree roots {values}.
        // creating this structure to optimize the GRAFT message count we send
        Multimap<InetAddress, InetAddress> graftTargets = null;
        long now = System.nanoTime();
        for (MissingMessges msgs : missing)
        {
            if (msgs.evaluationTimestamp > now)
                break;
            for (Map.Entry<InetAddress, MissingSummary> entry : msgs.trees.entrySet())
            {
                if (entry.getValue().messages.isEmpty())
                    continue;

            }
        }

        return graftTargets;
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

    /**
     * A simple struct to maintain the active and backup peer sets for a tree root (tree root is not a member of this class).
     */
    static class BroadcastPeers
    {
        final List<InetAddress> activePeers;
        final List<InetAddress> backupPeers;

        public BroadcastPeers(List<InetAddress> activePeers, List<InetAddress> backupPeers)
        {
            this.activePeers = activePeers;
            this.backupPeers = backupPeers;
        }

        public void addToActive(InetAddress peer)
        {
            //TODO:JEB test me
            if (!activePeers.contains(peer))
                activePeers.add(peer);
            backupPeers.remove(peer);
        }
        
        public void moveToBackup(InetAddress peer)
        {
            //TODO:JEB test me
            if (activePeers.remove(peer) && !backupPeers.contains(peer))
                backupPeers.add(peer);
        }

        public void remove(InetAddress peer)
        {
            //TODO:JEB test me
            activePeers.remove(peer);
            backupPeers.remove(peer);
        }
    }
}
