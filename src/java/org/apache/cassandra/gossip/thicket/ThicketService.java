/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;


/**
 * An implementation of <a href="http://asc.di.fct.unl.pt/~jleitao/pdf/srds10-mario.pdf">
 * Thicket: A Protocol for Maintaining Multiple Trees in a P2P Overlay</a>.
 * A thorough treatment is obviously in the paper, but sumamrized here. Thicket uses an underlying {@link PeerSamplingService}
 * to provide a view of the cluster from which it constructs dynamic spanning trees, upon which messages are broadcast.
 * This implementation will have one tree for each node in the cluster, thus simplifying a few (non-stated) assumptions in the paper.
 * Thicket attempts to have each node act as an interior node in some limited number of trees of the cluster; this helps to
 * reduce the load in a given node (see below for more details).
 *
 * Thicket maintains a set of active peers and a set of backup peers; initially all peers are in the backup set. The active set
 * maintains the collection of peers that messages are broadcast to (or received from), while the backup peers is used for populating
 * the active peers if it is not at capacity. The peers in the two sets are derived from the view provided by the {@link PeerSamplingService}.
 *
 * Messages:
 * - DATA - encapsulates the broadcast payload to be sent to the peers in the cluster.
 * - SUMMARY - a periodic broadcast to the peers in the backup set of all the recently received DATA message ids.
 * - GRAFT - sent when a node wants to repair a broken branch of the spanning tree, and connecting to the receiving peer
 * will heal the tree.
 * - PRUNE - sent by a node when it wants to be removed from the peer's active set (and put into it's backup set). Sent when a node
 * receives a duplicate DATA message or is declining a GRAFT request.
 *
 * Each message sent between peers will also carry along the sending node's {@link LoadEstimate}. That load estimate data can be used in the future
 * as prt of a best effort mechanism when healing broken tree branches.
 *
 * State machine
 * A nice way to think about the active and passive peers sets is to consider them like a state machine, wherein each node controls how peers
 * are allowed into node's (local) peers sets. We can consider a state machine for peers with four states:
 * - START - The beginning state. Any peers to be added to the thicket views are learned about from the underlying
 * {@link PeerSamplingService}, through either a call to {@link PeerSamplingService#getPeers()} or a
 * {@link PeerSamplingServiceListener#neighborUp(InetAddress)} notification, and then transition directly to the BACKUP state.
 * - BACKUP - A peer is stored in the backup peers set, and is sent SUMMARY messages as part of tree repair. A peer can be transitioned
 * into this state from the ACTIVE state when we receive PRUNE message from the peer. The PRUNE could be due to the peer receiving a
 * duplicate message, rejecting a GRAFT request due to it's load, and so on.
 * - ACTIVE - A node is stored in the active peers set, and sends/receives broadcast messages. A peer can be transitioned to the
 * ACTIVE state when it receives GRAFT or GRAFT_RESPONSE message (sent as part of the SUMMARY tree repair process).
 * - REMOVED - The peer has been removed both views. This happen when the underlying {@link PeerSamplingService} sends a
 * {@link PeerSamplingServiceListener#neighborDown(InetAddress)} notification, or the current node is shutting down.
 *
 * Is is important to point out that this state machine idea applies to each tree a node participates in - that is, for each node
 * in the cluster there is a separate tree, and so given node will have this state machine applied to each one.
 *
 * Tree healing (SUMMARY)
 *
 *
 *
 *
 *
 *
 *
 * Thread safety:
 * This class is *NOT* thread-safe, and intentionally so, in order to keep it simple and efficient.
 * With that restriction, though, is the requirement that all mutations to state *must* happen on the
 * primary thread.
 */
public class ThicketService implements BroadcastService, PeerSamplingServiceListener
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketService.class);

    /**
     * The time limit to retain message ids (for SUMMARY purposes), per tree-root.
     */
    private static final long MESSAGE_ID_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(40, TimeUnit.SECONDS);

    /**
     * The default time limit to retain data from SUMMARY messages sent to this node, per tree-root.
     */
    static final long SUMMARY_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);

    /**
     * The number of trees a node may be interior to (that is, not acting as a root or leaf in a given tree).
     */
    // TODO this value was unscientifically selected, and there's subtle handwaving in the paper: it claims
    // a node should be interior to only one tree, but the paper has a limited number of trees per cluster,
    // and also states "estimated to be interior nodes in a smaller number of trees" (section 4.3).
    private static final int INTERIORTITY_COUNT = 3;

    /**
     * A number of nanoseconds to retain data from SUMMARY messages sent to this node, per tree-root.
     */
    private final long summaryRetentionNanos;

    /**
     * The braodcast address of the local node. We memoize it here to avoid a hard dependency on {@link FBUtilities#getBroadcastAddress()}.
     */
    private final InetAddress localAddress;

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

    private volatile boolean executing;

    /**
     * Mapping of a message originator's address (the tree root) to the downstream branch peers from this node.
     *
     * "for each tree rooted at root, here is set the downstream peers from this node"
     */
    private final Map<InetAddress, BroadcastPeers> broadcastPeers;

    /**
     * A common set of backup peers to be used in determining downstream broadcast peers. More or less, this will be
     * equivalent to {@link PeerSamplingService#getPeers()} as it acts as a pool for the active view for each tree.
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
     * Collection of the most recently seen message ids sent via DATA messages. These entries will be published in SUMMARY messages
     * to other nodes, and then moved to {@code ThicketService#messagesLedger}, and then this list will be cleared for
     * the next batch of incoming messages.
     */
    private final List<TimestampedMessageId> receivedMessages;

    /**
     * A log of the message ids after they've been published to peers in SUMMARY messages. We keep them here for a limited time
     * so that we can successfully match against incoming SUMMARY messages to prove that we have indeed seen the message ids.
     * However, we can't keep the message ids around in memory forever, so we have to expunge them after some time.
     */
    private final List<TimestampedMessageId> messagesLedger;

    /**
     * Record of message ids that have been reported to this node via SUMMARY messages, but which we have not seen.
     * (In the Thicket paper, this data structure is called the 'announcements' set.) Missing messages are grouped by the
     * tree root to which the messages belong, and because branching is unique per-tree, we need to identify each tree uniquely
     * to determine which one is broken/partitioned/unhappy so we can repair the appropriate tree.
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

    public ThicketService(InetAddress localAddress, ExecutorService executor, ScheduledExecutorService scheduledTasks)
    {
        this(localAddress, executor, scheduledTasks, SUMMARY_RETENTION_TIME);
    }

    @VisibleForTesting
    ThicketService(InetAddress localAddress, ExecutorService executor,
                   ScheduledExecutorService scheduledTasks, long summaryRetentionNanos)
    {
        this.localAddress = localAddress;
        this.executor = executor;
        this.scheduledTasks = scheduledTasks;
        this.summaryRetentionNanos = summaryRetentionNanos;

        clients = new HashMap<>();
        receivedMessages = new LinkedList<>();
        messagesLedger = new LinkedList<>();
        missingMessages = new LinkedList<>();
        broadcastPeers = new HashMap<>();
        backupPeers = new HashSet<>();
        loadEstimates = HashMultimap.create();
    }

    public void start(PeerSamplingService peerSamplingService, int epoch)
    {
        start(peerSamplingService, epoch, secondsToMillis(10), secondsToMillis(1), secondsToMillis(10), secondsToMillis(10));
    }

    private static long secondsToMillis(int seconds)
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
        executing = true;
        this.peerSamplingService = peerSamplingService;
        idGenerator = new GossipMessageId.IdGenerator(epoch);
        backupPeers.addAll(peerSamplingService.getPeers());
        summarySender = scheduledTasks.scheduleWithFixedDelay(new SummarySender(), summaryStartMillis, summaryDelayMillis, TimeUnit.MILLISECONDS);
        missingMessagesResolver = scheduledTasks.scheduleWithFixedDelay(new MissingMessagesTimer(), missingMessageStartMiils, missingMessageDelayMiils, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    void testingPause()
    {
        executing = false;
    }

    @VisibleForTesting
    void testingRestart()
    {
        executing = true;
    }

    /**
     * Shut down the broadcast behaviors, and clear out some data structures. On process shutdown, this is not very interesting,
     * but if temporarily stopping gossip (via nodetool, for example) it will reset everything for starting up again.
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
     * Broadcast a client's message down through the spanning tree. The actual broadcast is scheduled into the internal,
     * single-threaded executor service.
     */
    public void broadcast(final Object payload, final BroadcastServiceClient client)
    {
        executor.execute(() -> performBroadcast(payload, client));
    }

    /**
     * Broadcast a client's message down through the spanning tree. If we detect that the number of first-level peers
     * is less than the fanout (probably because a node was shutdown or a partition occured or ...), select additional peers
     * from the {@link #backupPeers} to ensure a full fanout.
     */
    void performBroadcast(Object payload, BroadcastServiceClient client)
    {
        GossipMessageId messageId = idGenerator.generate();
        recordMessage(localAddress, messageId);

        // rebuild (or add to) the tree-root's peers if the size is less than the fanout
        BroadcastPeers peers = broadcastPeers.get(localAddress);
        int fanout = deriveFanout();
        if (peers == null || peers.active.size() < fanout)
        {
            Collection<InetAddress> currentActive = peers == null ? Collections.emptyList() : peers.active;
            peers = selectBroadcastPeers(localAddress, currentActive, backupPeers, fanout);
            broadcastPeers.put(localAddress, peers);
        }

        DataMessage msg = new DataMessage(localAddress, messageId, localAddress, payload, client.getClientName(), localLoadEstimate(broadcastPeers, localAddress));
        for (InetAddress peer : peers.active)
            MessagingService.instance().sendOneWay(msg.getMessageOut(), peer);
        broadcastedMessages.incrementAndGet();
    }

    /**
     * Select random peers from {@code peersPool} for the next level of peers in a broadcast tree;
     * remainder peers, if any, will become the backup set for that tree (used for tree healing).
     */
    @VisibleForTesting
    static BroadcastPeers selectBroadcastPeers(InetAddress treeRoot, Collection<InetAddress> currentPeers, Collection<InetAddress> peersPool, int maxActive)
    {
        Set<InetAddress> active = new HashSet<>(currentPeers);
        Set<InetAddress> backup = new HashSet<>();

        List<InetAddress> candidates = new ArrayList<>(peersPool);
        candidates.remove(treeRoot);
        Collections.shuffle(candidates);

        // a candidate should be in either the active or backup list, not both
        for (InetAddress peer : candidates)
        {
            if (active.contains(peer))
                continue;
            if (active.size() < maxActive)
                active.add(peer);
            else
                backup.add(peer);
        }

        return new BroadcastPeers(active, backup);
    }

    /**
     * Calculate the fanout size, this is, the number of peers that will be broadcast to when the this node has a new
     * message is wants to send. The fanout must be smaller than the {@link #backupPeers} size in order to heal
     * broken branches on a given tree-root.
     */
    private int deriveFanout()
    {
        int size = backupPeers.size();
        // NOTE: these values weren't scientifically calculated, but seemed reasonable based on comparisions with the thicket paper
        // and other broadcast tree gossip implementations

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

    private void recordMessage(InetAddress treeRoot, GossipMessageId messageId)
    {
        receivedMessages.add(new TimestampedMessageId(treeRoot, messageId, System.nanoTime() + MESSAGE_ID_RETENTION_TIME));
    }

    void receiveMessage(ThicketMessage message)
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

        // TODO should we also look at the message id, as well, in determining if the message is stale?
        // this would require keeping a history around (we kind of already do), similar to HyParView service does, but could also
        // require us to know something more more about the payload (which we don't want to know about)
        if (client.receive(message.payload))
        {
            relayMessage(message);
        }
        else
        {
            logger.info(String.format("%s sending PRUNE to %s due to duplicate message in tree %s", localAddress, message.sender, message.treeRoot));
            PruneMessage msg = new PruneMessage(localAddress, idGenerator.generate(), Collections.singletonList(message.treeRoot), localLoadEstimate(broadcastPeers, localAddress));
            MessagingService.instance().sendOneWay(msg.getMessageOut(), message.sender);
        }
    }

    /**
     * Remove an entry from the missing messages collection (for a given tree root). After removing, if there are no further
     * outstanding missing {@link GossipMessageId}s for the tree-root, remove it's entry from {@code missing}, as well.
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
     * Send a received DATA message to peers on downstream branches, if any.
     */
    void relayMessage(DataMessage message)
    {
        BroadcastPeers bcastPeers = broadcastPeers.get(message.treeRoot);
        Collection<InetAddress> peers = bcastPeers == null ? Collections.emptySet() : bcastPeers.active;

        // check if we are already a leaf in this tree
        if (peers.size() == 1 && peers.iterator().next().equals(message.sender))
        {
            // it would be nice capture the message.hopCount as a reportable metric (as we're a leaf)
            return;
        }

        if (peers.isEmpty())
        {
            if (isInterior(broadcastPeers, localAddress))
                bcastPeers = createLeafEntry(message.sender, backupPeers);
            else
                bcastPeers = selectBroadcastPeers(message.treeRoot, Collections.singletonList(message.sender), backupPeers, deriveFanout());

            broadcastPeers.put(message.treeRoot, bcastPeers);
            peers = bcastPeers.active;
        }

        DataMessage msg = new DataMessage(localAddress, localLoadEstimate(broadcastPeers, localAddress), message);
        peers.stream()
             .filter(peer -> !peer.equals(message.sender) && !peer.equals(message.treeRoot))
             .forEach(peer -> MessagingService.instance().sendOneWay(msg.getMessageOut(), peer));
    }

    /**
     * Check if the current node is interior to at least one tree in the cluster.
     */
    @VisibleForTesting
    static boolean isInterior(Map<InetAddress, BroadcastPeers> broadcastPeers, InetAddress localAddress)
    {
        for (Map.Entry<InetAddress, BroadcastPeers> entry : broadcastPeers.entrySet())
        {
            // ignore the entry for the localAddress (this node)

            if (entry.getValue().active.size() >= INTERIORTITY_COUNT  && !entry.getKey().equals(localAddress))
                return true;
        }
        return false;
    }

    private static BroadcastPeers createLeafEntry(InetAddress upstreamPeer, Collection<InetAddress> backupPeers)
    {
        BroadcastPeers broadcastPeers = new BroadcastPeers(new HashSet<>(), new HashSet<>(backupPeers));
        broadcastPeers.addToActive(upstreamPeer);
        return broadcastPeers;
    }

    private void runSummarize()
    {
        try
        {
            sendSummary();
        }
        catch (Exception e)
        {
            logger.error("error while trying to generate SUMMARY messages", e);
        }
    }

    /**
     * Invoked periodically, this will send a SUMMARY message containing all received message IDs to peers in the backup set,
     * assumming, of course, we have any recently received messages.
     *
     * In the Thicket paper, SUMMARY messages are sent to all nodes in the backup peers set. However, we treat the backup peers
     * differently from the paper (nodes are reused across tree-roots), and so we don't have a clean, easy to use set of peers.
     */
    void sendSummary()
    {
         if (!executing || receivedMessages.isEmpty())
            return;

        logger.debug("[] sendSummary() - found some receviedMessages", localAddress);
        Multimap<InetAddress, GossipMessageId> byTreeRoot = convert(receivedMessages);
        Map<InetAddress, Multimap<InetAddress, GossipMessageId>> targetedSummaries = findSummaryTargets(byTreeRoot, broadcastPeers);
        Collection<LoadEstimate> loadEstimates = localLoadEstimate(broadcastPeers, localAddress);
        logger.debug("{} sendSummary() - will send SUMMARY to {}", localAddress, targetedSummaries.keySet());

        for (Map.Entry<InetAddress, Multimap<InetAddress, GossipMessageId>> entry : targetedSummaries.entrySet())
        {
            SummaryMessage message = new SummaryMessage(localAddress, idGenerator.generate(), entry.getValue(), loadEstimates);
            MessagingService.instance().sendOneWay(message.getMessageOut(), entry.getKey());
        }

        pruneMessageLedger(messagesLedger);
        messagesLedger.addAll(receivedMessages);
        receivedMessages.clear();
    }

    /**
     * Create a view of the load estimate for this node that can be sent to peers. The return value will not contain
     * entries for trees where this node is a leaf (as that does not contribute to the load factor of the node),
     * nor will it include it's own tree data (where it acts as the tree-root).
     */
    @VisibleForTesting
    static Collection<LoadEstimate> localLoadEstimate(Map<InetAddress, BroadcastPeers> broadcastPeers, InetAddress localAddress)
    {
        List<LoadEstimate> estimates = new LinkedList<>();

        for (Map.Entry<InetAddress, BroadcastPeers> entry : broadcastPeers.entrySet())
        {
            if (entry.getValue().active.size() > 1 && !entry.getKey().equals(localAddress))
            {
                // subtract one from the size because we include the upstream peer (the one that sends us the message)
                // in the list, so remove it from the count as we don't send messages to it in this tree
                int load = entry.getValue().active.size() - 1;
                estimates.add(new LoadEstimate(entry.getKey(), load));
            }
        }

        return estimates;
    }

    /**
     * Regroup the {@code receivedMessages} to a mapping of {tree-root -> messageIds}.
     */
    private static Multimap<InetAddress, GossipMessageId> convert(List<TimestampedMessageId> receivedMessages)
    {
        Multimap<InetAddress, GossipMessageId> result = HashMultimap.create();
        for (TimestampedMessageId msgId : receivedMessages)
            result.put(msgId.treeRoot, msgId.messageId);
        return result;
    }

    private static Map<InetAddress, Multimap<InetAddress, GossipMessageId>> findSummaryTargets(Multimap<InetAddress, GossipMessageId> byTreeRoots,
                                                                                               Map<InetAddress, BroadcastPeers> broadcastPeers)
    {
        Map<InetAddress, Multimap<InetAddress, GossipMessageId>> summariesPerTraget = new HashMap<>();

        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : byTreeRoots.asMap().entrySet())
        {
            BroadcastPeers peers = broadcastPeers.get(entry.getKey());

            // if we've lost the peers entry for the tree (maybe it's unavailable/down/out of our peer sampling service view)
            // or we're already at maxLoad for the tree, we can't help any peer repair it's branch for the tree, so ignore
            if (peers == null || peers.backup.isEmpty())
                continue;

            for (InetAddress peer : peers.backup)
            {
                Multimap<InetAddress, GossipMessageId> messagesForTarget = summariesPerTraget.get(peer);
                if (messagesForTarget == null)
                {
                    messagesForTarget = HashMultimap.create();
                    summariesPerTraget.put(peer, messagesForTarget);
                }
                messagesForTarget.putAll(entry.getKey(), entry.getValue());
            }
        }

        return summariesPerTraget;
    }

    static void pruneMessageLedger(List<TimestampedMessageId> messagesLedger)
    {
        // perhaps we should bound the overall number of messages in the ledger, as well?

        // since this class is single threaded, we can assume the messages are in arrival timestamp order
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
     * entire tree is spanned).
     */
    void handleSummary(SummaryMessage msg)
    {
        recordLoadEstimates(loadEstimates, msg.sender, msg.estimates);

        Multimap<InetAddress, GossipMessageId> reportedMissing = filterMissingMessages(msg.receivedMessages, mergeSeenMessages());

        if (!reportedMissing.isEmpty())
            addToMissingMessages(msg.sender, missingMessages, reportedMissing, summaryRetentionNanos);
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
     * Filter out messages the local node has already recieved.
     *
     * @return missing {@link GossipMessageId}s mapped to their tree root.
     */
    @VisibleForTesting
    Multimap<InetAddress, GossipMessageId> filterMissingMessages(Multimap<InetAddress, GossipMessageId> summary,
                                                                 Multimap<InetAddress, GossipMessageId> seenMessagesPerTreeRoot)
    {
        Multimap<InetAddress, GossipMessageId> filtered = HashMultimap.create();

        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : summary.asMap().entrySet())
        {
            // probably reasonable to assume we've seen our own messages
            InetAddress peer = entry.getKey();
            if (peer.equals(localAddress) && (entry.getValue() != null))
                continue;

            List<GossipMessageId> ids = new ArrayList<>(entry.getValue());
            if (seenMessagesPerTreeRoot.containsKey(peer))
                ids.removeAll(seenMessagesPerTreeRoot.get(peer));

            if (!ids.isEmpty())
                filtered.putAll(peer, ids);
        }

        return filtered;
    }

    /**
     * Add missing message ids to existing {@code missingMessages} instances (indexed by tree-root), and if any
     * {@code reportedMissing} entries remain, add new structs to the end of the list.
     */
    @VisibleForTesting
    static void addToMissingMessages(InetAddress sender, List<MissingMessges> missingMessages,
                                     Multimap<InetAddress, GossipMessageId> reportedMissing, long summaryRetentionNanos)
    {
        // add to existing MissingMessages instances
        for (MissingMessges missingMessges : missingMessages)
        {
            for (Map.Entry<InetAddress, MissingSummary> entry : missingMessges.trees.entrySet())
            {
                Collection<GossipMessageId> ids = reportedMissing.removeAll(entry.getKey());
                if (ids != null && !ids.isEmpty())
                    entry.getValue().add(sender, ids);
            }
        }

        if (reportedMissing.isEmpty())
            return;

        // now, add to new MissingMessages instance
        MissingMessges msgs = new MissingMessges(System.nanoTime() + summaryRetentionNanos);
        for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : reportedMissing.asMap().entrySet())
        {
            MissingSummary missingSummary = new MissingSummary();
            missingSummary.add(sender, entry.getValue());
            msgs.trees.put(entry.getKey(), missingSummary);
        }
        missingMessages.add(msgs);
    }

    private void runCheckMissingMessages()
    {
        try
        {
            checkMissingMessages();
        }
        catch (Exception e)
        {
            logger.error("error while checking missing messages in thicket", e);
        }
    }

    /**
     * A periodically invoked method that examines the missing messages set that we've recieved via SUMMARY messages, and if
     * those message ids are still outstanding, attempts to heal broken branches by GRAFTing peers for the message-specific tree-root.
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
        Map<InetAddress, InetAddress> treeRootGrafts = new HashMap<>();
        // might be more efficient wrt the number of messages sent to group the graftCandidates by tree-root targeted to the same peer
        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftCandidates.asMap().entrySet())
        {
            Optional<InetAddress> target = detetmineBestCandidate(loadEstimates, entry.getValue(), maxLoad);
            if (!target.isPresent())
            {
                // we know we've got a broken branch, and the SUMMARY candidates are not looking good,
                // so let's go for broke and try any other reasonable node for this tree
                List<InetAddress> backups = new ArrayList<>(backupPeers);
                backups.removeAll(entry.getValue());
                BroadcastPeers peers = broadcastPeers.get(entry.getKey());
                if (peers != null)
                    backups.removeAll(peers.active);

                target = detetmineBestCandidate(loadEstimates, backups, maxLoad);
                if (target.isPresent())
                    logger.info("**** found a last-ditch GRAFT target! " + target.get());
            }

            if (target.isPresent())
            {
                logger.info(String.format("checkMissingMessages(): %s - sending GRAFT req to %s", localAddress, target));
                GraftMessage msg =  new GraftMessage(localAddress, idGenerator.generate(), Collections.singletonList(entry.getKey()), localLoadEstimate(broadcastPeers, localAddress));
                MessagingService.instance().sendOneWay(msg.getMessageOut(), target.get());
                treeRootGrafts.put(entry.getKey(), target.get());
            }
            else
            {
                logger.warn(String.format("checkMissingMessages(): %s - could not find GRAFT target from SYMAMRY senders %s", localAddress, entry.getValue()));
            }
        }

        removeProcessed(missingMessages, graftCandidates.keySet());
        graftSummaryTargets(broadcastPeers, treeRootGrafts);
    }

    /**
     * For each tree-root with missing messages, find potential peers to GRAFT onto.
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
            // with a higher timerstamp, we're done iterating
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


    /**
     * Determine the maxLoad for forwarding DATA messages to peers.
     *
     * A careful reading of the thicket and hyparview papers will reveal that thicket's maxLoad value should be
     * "logarithmic with the number of nodes in the system"; while hyparview's active view (which becomes thicket's
     * backupPeers set) is "log(n) + c", where n is the number of nodes in the system and c is a constant, usually 1
     * (according to the experiments in the paper). Thus, we would end up with a maxLoad value one less than the size
     * of the backupPeers size - which means there would always be an entry we could never make use of.
     *
     * Because of all this, maxLoad is simply the size of the backupPeers.
     */
    private int deriveMaxLoad()
    {
        return peerSamplingService.getPeers().size();
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

        if (filtered.isEmpty())
            return Optional.empty();
        if (filtered.size() == 1)
            return Optional.of(filtered.get(0));

        Collections.shuffle(filtered);
        int lowestCount = Integer.MAX_VALUE;
        InetAddress peer = null;
        for (InetAddress addr : filtered)
        {
            int cnt = loadEstimates.get(addr).stream().mapToInt(loadEstimate -> loadEstimate.load).sum();
            if (peer == null || cnt < lowestCount)
            {
                lowestCount = cnt;
                peer = addr;
            }
        }
        return Optional.of(peer);
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
        int load = loadEstimates.stream().mapToInt(loadEstimate -> loadEstimate.load).sum();
        return load >= maxLoad || loadEstimates.size() >= INTERIORTITY_COUNT;
    }

    private static void removeProcessed(List<MissingMessges> missingMessages, Collection<InetAddress> processedTreeRoots)
    {
        for (InetAddress treeRoot : processedTreeRoots)
        {
            for (MissingMessges missingMessge : missingMessages)
            {
                if (missingMessge.trees.remove(treeRoot) != null)
                    break;
            }
        }
    }

    private void graftSummaryTargets(Map<InetAddress, BroadcastPeers> broadcastPeers, Map<InetAddress, InetAddress> treeRootGrafts)
    {
        for (Map.Entry<InetAddress, InetAddress> entry : treeRootGrafts.entrySet())
        {
            BroadcastPeers peers = broadcastPeers.get(entry.getKey());
            if (peers != null)
            {
                peers.addToActive(entry.getValue());
                return;
            }

            peers = createLeafEntry(entry.getValue(), backupPeers);
            broadcastPeers.put(entry.getKey(), peers);
        }
    }

    /**
     * Process an incoming GRAFT message. If we have not exceeded the maxLoad limit on this node, go ahead and accept
     * the GRAFT request and heal the tree by adding the sender to the broadcastPeers for the given tree-roots.
     * Else if we are over the maxLoad limit, send a PRUNE message back to the sender (maybe it will have better luck next time
     * finding a peer to heal it's branch).
     */
    void handleGraft(GraftMessage message)
    {
        recordLoadEstimates(loadEstimates, message.sender, message.estimates);

        Collection<LoadEstimate> loadEstimates = localLoadEstimate(broadcastPeers, localAddress);
        if (!isOverMaxLoad(loadEstimates, deriveMaxLoad()))
        {
            for (InetAddress treeRoot : message.treeRoots)
            {
                BroadcastPeers peers = broadcastPeers.get(treeRoot);
                if (peers == null)
                    continue;
                peers.addToActive(message.sender);
            }

            // TODO:JEB kick off 'job' to transfer missing data to peer (as identified by the missing message ids)
        }
        else
        {
            logger.debug(String.format("%s sending PRUNE to %s due to denied GRAFT", localAddress, message.sender));
            PruneMessage msg = new PruneMessage(localAddress, idGenerator.generate(), message.treeRoots, loadEstimates);
            MessagingService.instance().sendOneWay(msg.getMessageOut(), message.sender);
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
            BroadcastPeers branchPeers = broadcastPeers.get(treeRoot);
            if (branchPeers != null)
            {
                branchPeers.moveToBackup(message.sender);
            }
        }
    }

    @Override
    public void register(BroadcastServiceClient client)
    {
        clients.put(client.getClientName(), client);
        DataMessage.clients.put(client.getClientName(), client);
    }

    @Override
    public void neighborUp(InetAddress peer)
    {
        if (backupPeers.contains(peer))
            return;

        backupPeers.add(peer);

        // add the new peer as a backup in every existing tree
        for (BroadcastPeers peers : broadcastPeers.values())
            peers.backup.add(peer);
    }

    @Override
    public void neighborDown(InetAddress peer)
    {
        backupPeers.remove(peer);
        broadcastPeers.remove(peer);
        for (InetAddress treeRoot : broadcastPeers.keySet())
            broadcastPeers.remove(treeRoot, peer);

        // a given peer will only occur once in the missingMessages collection
        for (MissingMessges missingMessage : missingMessages)
        {
            if (missingMessage.trees.remove(peer) != null)
                break;
        }
        loadEstimates.removeAll(peer);
    }

    private class SummarySender implements Runnable
    {
        @Override
        public void run()
        {
            executor.execute(ThicketService.this::runSummarize);
        }
    }

    private class MissingMessagesTimer implements Runnable
    {
        @Override
        public void run()
        {
            executor.execute(ThicketService.this::runCheckMissingMessages);
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
    Map<InetAddress, BroadcastPeers> getBroadcastPeers()
    {
        return broadcastPeers;
    }

    @VisibleForTesting
    int getBroadcastedMessageCount()
    {
        return broadcastedMessages.get();
    }

    /**
     * A simple struct to capture metadata about recently received messages from peers.
     */
    private static class TimestampedMessageId
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
     * A struct which contains missing {@link GossipMessageId}s, per tree-root. All missing message ids for a given tree root are captured
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
     * A struct to hold the missing {@link GossipMessageId}s and the peers who informed us of those. Ultimately, it doesn't
     * matter so much which peer reported the ids, as any of them still received informatation before this node did.
     */
    static class MissingSummary
    {
        /**
         * Total set of missing message ids (for a given tree) from all peers that sent SUMMARY messages.
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
     * Maintains the peers in active and backup sets for a tree in which this node is a participant.
     */
    static class BroadcastPeers
    {
        final Set<InetAddress> active;
        final Set<InetAddress> backup;

        BroadcastPeers(Set<InetAddress> active, Set<InetAddress> backup)
        {
            this.active = active;
            this.backup = backup;
        }

        void moveToBackup(InetAddress peer)
        {
            active.remove(peer);
            backup.add(peer);
        }

        void addToActive(InetAddress peer)
        {
            active.add(peer);
            backup.remove(peer);
        }

        @Override
        public String toString()
        {
            return String.format("active: %s, backup: %s", active, backup);
        }
    }
}
