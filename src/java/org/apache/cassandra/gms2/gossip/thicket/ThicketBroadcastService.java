package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.gms2.gossip.thicket.messages.GraftRequestMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.GraftResponseAcceptMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.GraftResponseRejectMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.PruneMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.SummaryMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketDataMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.ExpiringMap.CacheableObject;
import org.apache.cassandra.utils.Pair;

/**
 * An implementation of the
 * <a html="http://asc.di.fct.unl.pt/~jleitao/pdf/srds10-mario.pdf">
 * Thicket: A Protocol for Building and Maintaining Multiple Trees in a P2P Overlay</a> paper.
 */
public class ThicketBroadcastService<M extends ThicketMessage> implements GossipBroadcaster, GossipDispatcher.GossipReceiver<M>
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketBroadcastService.class);
    private static final int MAX_GRAFT_ATTEMPTS = 2;
    private static final long ANNOUNCEMENTS_ENTRY_TTL = TimeUnit.SECONDS.toMillis(10);

    //TODO: fix this hard-coded value
    private static final int MAX_LOAD = 1000;

    private final ThicketConfig config;
    private final GossipDispatcher dispatcher;

    /**
     * A mapping of tree root node -> broadcast targets.
     */
    private final ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers;

    /**
     * A cache of the {@code activePeers} field as needed for serialization. This does not need to be perfectly in sync,
     * just not null when dereferencing, so it's ok if there are concurrent modifications to the {@code activePeers}
     * not represented here when writing to the wire.
     */
    private Map<InetAddress, Integer> loadEstimate;

    /**
     * Collection of peers from which to draw when contacted by a new {sender/tree root} combination.
     * These entries would become the target peers for broadcasting.
     */
    private final Collection<InetAddress> backupPeers;

    /**
     * Maintains the load estimates as broadcasted by other nodes. The key of the map is the peer's address, and the value contains
     * a map of tree roots to the number of branches the node has in that tree. As we receive a full and complete map from each peer
     * on every interaction, we can do a full map replacement with what deserialize off the wire, instead of maintaining a curated map
     * in a MultiMap.
     *
     * For large clusters, I don't think we need to worry about this structure growing to cover the entire set of nodes as the
     * peer sampling sampling service should give us a limited view of the cluster. However, in the face of network partitions
     * and cluster-wide, rolling restarts on nodes that have a long uptime, YMMV.
     *
     * NOTE: not sure I like having yet another data structure hanging about, solely for the load estimates.
     * however, we do want to keep around load estimates after peers come and go from the active and backup lists (I think...)
     */
    private final ConcurrentMap<InetAddress, Map<InetAddress, Integer>> loadEstimates;

    private final ConcurrentMap<String, BroadcastClient> clients;

    /**
     * A collection of recently received broadcast messages per each {@code BroadcastClient}. These will be sent to a peer during the
     * Summary sessions, as per the Thicket paper.
     *
     */
    //TODO: might want an atomic reference around the value, so we can easily swap it out.
    private final ConcurrentMap<String, HashMap<ReceivedMessage, InetAddress>> recentMessages;

    // need tuple of {clientId, messageId, (treeRoot, sender)}
    private final ExpiringMap<ExpiringMapEntry, CopyOnWriteArrayList<InetAddress>> announcements;

    /**
     * The maximum number of peers to add as downstream tree nodes (in an activePeers set).
     */
    private int fanout = 1;

    private final AntiEntropyPeerListener stateChangeSubscriber;

    public ThicketBroadcastService(ThicketConfig config, GossipDispatcher dispatcher)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        activePeers = new ConcurrentHashMap<>();
        loadEstimates = new ConcurrentHashMap<>();

        // should this be a set instead? not sure if we care about FIFO properties... (we don't)
        backupPeers = new CopyOnWriteArrayList<>();
        clients = new ConcurrentHashMap<>();
        stateChangeSubscriber = new AntiEntropyPeerListener();
        recentMessages = new ConcurrentHashMap<>();
        announcements = new ExpiringMap<>(ANNOUNCEMENTS_ENTRY_TTL, new AnnouncementTimeoutProcessor());
    }

    public void init(ScheduledExecutorService scheduledService)
    {
        scheduledService.scheduleAtFixedRate(new SummaryTask(), 20, 10, TimeUnit.SECONDS);
    }

    public void broadcast(String clientId, Object messageId, Object message)
    {
        InetAddress localAddr = config.getLocalAddr();
        Collection<InetAddress> targets = getTargets(null, localAddr);

        if (!targets.isEmpty())
        {
            ThicketDataMessage dataMessage = new ThicketDataMessage(localAddr, clientId, messageId, message, buildLoadEstimate());

            for (InetAddress addr : targets)
                dispatcher.send(this, dataMessage, addr);

            addToRecentMessages(clientId, messageId, localAddr, localAddr);
        }
        else
        {
            // TODO: possibly queue - unless cluster of one node (as in local testing)
        }
    }

    private void addToRecentMessages(String clientId, Object messageId, InetAddress treeRoot, InetAddress sender)
    {
        HashMap<ReceivedMessage, InetAddress> msgs = recentMessages.get(clientId);
        if (msgs == null)
        {
            msgs = new HashMap<>();
            HashMap<ReceivedMessage, InetAddress> existing = recentMessages.putIfAbsent(clientId, msgs);
            if (existing != null)
                msgs = existing;
        }

        // if there is a race here (two concurrent invocations from handleData()), that's ok, as long peer as one wins.
        // soon enough, we'll get messages from the competing peers staggered enough in time that we won't race
        // and the '4.4 Tree reconfiguration' code will kick in
        msgs.put(new ReceivedMessage(messageId, treeRoot), sender);
    }

    private Map<InetAddress, Integer> buildLoadEstimate()
    {
        Map<InetAddress, Integer> loadMap = this.loadEstimate;
        if (loadMap != null)
            return loadMap;

        loadMap = new HashMap<>();
        for (Map.Entry<InetAddress, CopyOnWriteArraySet<InetAddress>> entry : activePeers.entrySet())
        {
            CopyOnWriteArraySet<InetAddress> branches = entry.getValue();
            if (branches == null || branches.size() == 0)
                continue;
            loadMap.put(entry.getKey(), branches.size());
        }

        // it's ok if there's a slight race here, we assume another thread built, more or less, the same map
        if (loadEstimate == null)
            loadEstimate = loadMap;
        return loadMap;
    }

    /**
     * @param sender May be null is we are processing for the tree root (determining the first-degree branches).
     * @param treeRoot The node the tree is rooted at
     */
    Collection<InetAddress> getTargets(InetAddress sender, InetAddress treeRoot)
    {
        CopyOnWriteArraySet<InetAddress> targets = activePeers.get(treeRoot);
        if (targets != null)
        {
            // this is to catch case where we get message from a node (in the same rooted tree) not from the usual sender
            if (sender != null && !targets.contains(sender))
            {
                targets.add(sender);
                loadEstimate = null;
            }
            return targets;
        }

        // if sender is null, it means we're at the tree root
        List<InetAddress> peers;
        if (sender == null)
        {
            peers = new ArrayList<>(fanout);
            Utils.selectMultipleRandom(backupPeers, peers, fanout);
        }
        else if (isInterior())
        {
            // if this node is already interior to some other tree, the activePeers for the new tree (for this node)
            // will only contain the sender (so this node acts as a leaf)
            peers = new ArrayList<>(1);
            peers.add(sender);
        }
        else
        {
            peers = new ArrayList<>(fanout);
            peers.add(sender);

            // exclude tree root from the results list if the sender was not the tree root itself
            List<InetAddress> filter;
            if (!sender.equals(treeRoot))
                filter = Collections.singletonList(treeRoot);
            else
                filter = Collections.emptyList();

            // subtract one from the fanout as we've already included the sender
            Utils.selectMultipleRandom(backupPeers, peers, filter, fanout - 1);
        }

        targets = new CopyOnWriteArraySet<>(peers);
        CopyOnWriteArraySet<InetAddress> existing = activePeers.putIfAbsent(treeRoot, targets);
        if (existing != null)
            return existing;
        loadEstimate = null;
        return targets;
    }

    boolean isInterior()
    {
        // NOTE: we are looking to find any set of peers, rooted at any node, where the count is greater than one.
        // as we always include the sender (rooted at a node) in the activePeers for a tree, we can identify if we're a
        // leaf node of that tree by the activePeer's set size being equal to one.
        for (CopyOnWriteArraySet<InetAddress> branches : activePeers.values())
        {
            if (branches.size() > 1)
                return true;
        }
        return false;
    }

    public void handle(ThicketMessage msg, InetAddress sender)
    {
        // capture the load estimate, as per the thicket paper
        loadEstimates.put(sender, msg.getLoadEstimate());
        try
        {
            switch (msg.getMessageType())
            {
                case DATA:
                    handleDataMessage((ThicketDataMessage) msg, sender);
                    break;
                case PRUNE:
                    handlePrune((PruneMessage) msg, sender);
                    break;
                case SUMMARY:
                    handleSummary((SummaryMessage) msg, sender);
                    break;
                case GRAFT_REQUEST:
                    handleGraftRequest((GraftRequestMessage) msg, sender);
                    break;
                case GRAFT_RESPONSE_ACCEPT:
                    handleGraftResponseAccept((GraftResponseAcceptMessage) msg, sender);
                    break;
                case GRAFT_RESPONSE_REJECT:
                    handleGraftResponseReject((GraftResponseRejectMessage) msg, sender);
                    break;
                default:
                    throw new IllegalArgumentException("unknown/unhandled thicket message type: " + msg.getMessageType());
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to handle thicket message", e);
        }
    }

    void handleDataMessage(ThicketDataMessage msg, InetAddress sender) throws IOException
    {
        BroadcastClient client = clients.get(msg.getClientId());
        if (client == null)
        {
            System.out.println(String.format("%s, received a message for unknown client component: %s; ignoring message", getAddress(), msg.getClientId()));
            return;
        }

        // check if there are any open timers waiting for this message; if so, cancel them, and remove from 'announcements'
        CopyOnWriteArrayList<InetAddress> previousSenders = cancelAnnouncement(msg.getClientId(), msg.getMessageId(), msg.getTreeRoot());

        if (client.receiveBroadcast(msg.getMessageId(), msg.getMessage()))
        {
            Collection<InetAddress> targets = getTargets(sender, msg.getTreeRoot());
            ThicketDataMessage dataMessage = new ThicketDataMessage(msg, buildLoadEstimate());
            for (InetAddress addr : targets)
            {
                if (!addr.equals(sender))
                    dispatcher.send(this, dataMessage, addr);
            }
            maybeReconfigure(msg.getClientId(), msg.getTreeRoot(), sender, previousSenders);
            addToRecentMessages(msg.getClientId(), msg.getMessageId(), msg.getTreeRoot(), sender);
        }
        else
        {
            //TODO: what should we do when tree root is the same as the sender?
            // it's possible that treeRoot broadcasted to one node (A), then paused (for whatever reason) -
            // and during that pause node A sent the message to B (due to various graft/prune mechanics),
            // and B is a direct target of the treeRoot. Then the treeRoot recovers, and broadcasts to B -
            // B will see the message is a dupe, but should it really prune the treeRoot from it's own tree?

            removeActivePeer(msg.getTreeRoot(), sender);
            dispatcher.send(this, new PruneMessage(msg.getTreeRoot(), buildLoadEstimate()), sender);
        }
    }

    /**
     * Based on Section 4.4 of the Thicket paper, optimize the tree based on faster senders.
     * If we receive a non-redundant message for which we already have an open announcement,
     * maybe PRUNE the sender of the message and GRAFT the summary sender of the announcement.
     */
    void maybeReconfigure(String clientId, InetAddress treeRoot, InetAddress sender, CopyOnWriteArrayList<InetAddress> previousSenders)
    {
        //TODO: write tests for me

        // check for open announcement
        if (previousSenders == null || previousSenders.isEmpty())
            return;

        // check if sender shipped us a summary before the message (really shouldn't happen, except in small clusters)
        if (previousSenders.contains(sender))
            return;

        InetAddress firstSender = previousSenders.get(0);
        Integer loadEstFirstSender = loadEstimate.get(firstSender);
        Integer loadEstSecondSender = loadEstimate.get(sender);
        if (loadEstFirstSender == null || loadEstSecondSender == null)
        {
            logger.warn("failed to capture load estimate information for a peer");
            return;
        }

        if (loadEstSecondSender > loadEstFirstSender)
        {
            removeActivePeer(treeRoot, sender);
            dispatcher.send(this, new PruneMessage(treeRoot, buildLoadEstimate()), sender);
            addToActivePeers(activePeers, treeRoot, firstSender);
            dispatcher.send(this, new GraftRequestMessage(treeRoot, clientId, 0, buildLoadEstimate()), firstSender);
        }
    }

    private CopyOnWriteArrayList<InetAddress> cancelAnnouncement(String clientId, Object messageId, InetAddress treeRoot)
    {
        ExpiringMapEntry entry = new ExpiringMapEntry(clientId, messageId, treeRoot);
        return announcements.remove(entry);
    }

    void removeActivePeer(InetAddress treeRoot, InetAddress toRemove)
    {
        //TODO: what should we do when tree root is the same as the sender?
        CopyOnWriteArraySet<InetAddress> branches = activePeers.get(treeRoot);
        if (branches != null)
            branches.remove(toRemove);
        backupPeers.add(toRemove);
    }

    /**
     * Remove the downstream peer from the tree rooted at the node indicated in the message.
     */
    void handlePrune(PruneMessage msg, InetAddress sender)
    {
        removeActivePeer(msg.getTreeRoot(), sender);
    }

    /**
     * Start a new summary session. If we are at or over the maxLoad parameter, skip doing a summary.
     * The reason being that if a peer doesn't have the updates, it's tree is broken. it will then ask
     * this node to graft, but as we're already over the maxLoad, we'd have to deny the request. I
     * In other words, we can't help others if we're over the maxLoad limit
     */
    void doSummary()
    {
        // don't perform a SUMMARY round if we're over the maxLoad, as we won't be able to help in the tree repair
        if (calculateForwardingLoad(loadEstimate) >= MAX_LOAD)
            return;

        Collection<InetAddress> destinations = summaryDestinations();

        // TODO: might want to limit the number of open summary sessions, as well as open sessions per peer (optimizations, i guess)

        // should we do a summary for *all* registered clients, ot just one? (to ease local and remote computational burden)
        // for now, I'm just shipping out for all of them - so yeah, a total of (numberOfClients x destinations) messages
        for (BroadcastClient client : clients.values())
        {
            HashMap<ReceivedMessage, InetAddress> msgs = recentMessages.remove(client.getClientId());

            //TODO: what tree root to use here?!?!?!?!?!
            SummaryMessage msg = new SummaryMessage(null, client.getClientId(), msgs.keySet(), client.prepareSummary(), buildLoadEstimate());
            for (InetAddress addr : destinations)
                dispatcher.send(this, msg, addr);
        }
    }

    private Collection<InetAddress> summaryDestinations()
    {
        Collection<InetAddress> peers = stateChangeSubscriber.selectNodes(fanout);
        if (!peers.isEmpty())
            return peers;

        // if the anti-entropy list is empty (possibly a small cluster), just select from the backup peers
        Utils.selectMultipleRandom(backupPeers, peers, fanout);

        // should we handle the case where the list is *still* empty?

        return peers;
    }

    void handleSummary(SummaryMessage msg, InetAddress sender)
    {
        String clientId = msg.getClientId();
        BroadcastClient client = clients.get(clientId);
        if (client == null)
        {
            logger.warn("received a summary request for an unknown client component: {}, ignoring", msg.getClientId());
            return;
        }

        // perform ant anti-entropy (push-only style)
        Set<? extends Object> missing = client.receiveSummary(msg.getSummary());

        for (Object msgId : missing)
        {
            // double check to see if the msg has come in
            if (client.hasReceivedMessage(msgId))
                continue;
            ExpiringMapEntry entry = new ExpiringMapEntry(clientId, msgId, msg.getTreeRoot());
            CopyOnWriteArrayList<InetAddress> senders = new CopyOnWriteArrayList<>();
            senders.add(sender);

            CopyOnWriteArrayList<InetAddress> previous = announcements.putIfAbsent(entry, senders);
            if (previous != null && !previous.contains(sender))
                previous.add(sender);
        }
    }

    void handleGraftRequest(GraftRequestMessage msg, InetAddress sender)
    {
        //TODO: make sure that by accepting this graft, it won't make us interior to more than one tree
        if (calculateForwardingLoad(loadEstimate) < MAX_LOAD)
        {
            addToActivePeers(activePeers, msg.getTreeRoot(), sender);

            // if we're bothering to accept a GRAFT, the requester was missing some data that this node already has
            // (and that the node still wants). Thus it makes sense to send over an anti-entropy summary, as well, so the requester
            // can converge quicker.
            BroadcastClient client = clients.get(msg.getClientId());
            dispatcher.send(this,
                            new GraftResponseAcceptMessage(msg.getTreeRoot(), msg.getClientId(), client.prepareSummary(), buildLoadEstimate()),
                           sender);
        }
        else
        {
            InetAddress alternate = null;
            CopyOnWriteArraySet<InetAddress> branches = activePeers.get(msg.getTreeRoot());
            if (branches != null && !branches.isEmpty())
            {
                List<InetAddress> peers = new ArrayList<>(branches);
                peers.remove(sender);  // sender should not be in list, but just in case
                alternate = findGraftAlternate(peers, loadEstimates);
            }
            dispatcher.send(this,
                            new GraftResponseRejectMessage(msg.getTreeRoot(), msg.getClientId(), msg.getAttemptCount(), alternate, buildLoadEstimate()),
                            sender);
        }
    }

    // TODO: this is pretty similar to getTargets() - should be combined?
    void addToActivePeers(ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers, InetAddress treeRoot, InetAddress addr)
    {
        //TODO: write tests for me

        if (addr == null)
            return;

        CopyOnWriteArraySet<InetAddress> targets = activePeers.get(treeRoot);
        if (targets != null)
        {
            // this is to catch case where we get message from a node (in the same rooted tree) not from the usual sender
            if (addr != null && !targets.contains(addr))
                targets.add(addr);
        }
        else
        {
            targets = new CopyOnWriteArraySet<>(Collections.singletonList(addr));
            CopyOnWriteArraySet<InetAddress> existing = activePeers.putIfAbsent(treeRoot, targets);
            if (existing != null)
                existing.add(addr);
        }
        loadEstimate = null;
    }

    /**
     * Find an alternate peer that a GRAFT requestor can contact due to a rejection from this node.
     * A peer is determined looking at this node's branches in the rooted tree, and selecting the one
     * with the lowest loadEstimate.
     */
    InetAddress findGraftAlternate(List<InetAddress> peers, ConcurrentMap<InetAddress, Map<InetAddress, Integer>> loadEstimates)
    {
        //TODO: write tests for me

        // If, for some reason, we don't have any load information for a branch, keep it in a separate field (unknownLoad).
        // if there's no other candidates when we're done searching, just use that node.
        InetAddress bestAddr = null, unknownLoad = null;
        int minLoadEstimate = Integer.MAX_VALUE;
        for (InetAddress addr : peers)
        {
            int loadEst = calculateForwardingLoad(loadEstimates.get(addr));

            if (loadEst == 0)
                unknownLoad = addr;
            else if (bestAddr ==  null || loadEst < minLoadEstimate)
            {
                bestAddr = addr;
                minLoadEstimate = loadEst;
            }
        }

        return bestAddr != null ? bestAddr : unknownLoad;
    }

    int calculateForwardingLoad(Map<InetAddress, Integer> loads)
    {
        if (loads == null || loads.isEmpty())
            return 0;

        int load = 0;
        for (Integer i : loads.values())
        {
            if (i == null)
                continue;
            load += i;
        }
        return load;
    }

    /**
     * When the timer for an announcement expires, we have to assume the sender (rooted at a given
     * tree) is down, so we need to replace it. To do that, we select one of the peers that sent
     * a SUMMARY request with the given {client, messageId, treeRoot} tuple, optimistically add it
     * to the local tree (for the tree root), and send a GRAFT message.
     *
     * @param clientId
     * @param messageId
     * @param treeRoot
     * @param summarySenders set of peers that sent this node a SUMMARY message with the given
     *                       {clientId, messageId} tuple.
     */
    void handleExpiredAnnouncement(String clientId, Object messageId, InetAddress treeRoot, CopyOnWriteArrayList<InetAddress> summarySenders)
    {
        //TODO: add tests for me

        BroadcastClient client = clients.get(clientId);
        if (client == null)
        {
            logger.warn(String.format("%s, processing an expired announcement for unknown client component: %s; ignoring",
                                      getAddress(), clientId));
            return;
        }

        // check to see if there was any race between the timer expiring and the client receiving and precessing the message
        if (client.hasReceivedMessage(messageId))
            return;

        // now we need to send out a GRAFT message, assumming the original sender is not reachable
        if (summarySenders.isEmpty())
        {
            logger.warn("wanted to send a GRAFT message in response to expired announcement, but summarySenders is empty");
            return;
        }

        InetAddress graftTarget = Utils.selectRandom(summarySenders);
        addToActivePeers(activePeers, treeRoot, graftTarget);
        dispatcher.send(this, new GraftRequestMessage(treeRoot, clientId, 0, buildLoadEstimate()), graftTarget);
    }

    void handleGraftResponseAccept(GraftResponseAcceptMessage msg, InetAddress sender)
    {
        // we've already done the optimistic work in the summary handling, so just process the summary
        BroadcastClient client = clients.get(msg.getClientId());
        if (client == null)
        {
            System.out.println(String.format("%s, received a message for unknown client component: %s; ignoring message", getAddress(), msg.getClientId()));
            return;
        }
        client.receiveSummary(msg.getSummary());
    }

    void handleGraftResponseReject(GraftResponseRejectMessage msg, InetAddress sender)
    {
        removeActivePeer(msg.getTreeRoot(), sender);

        int attemptCount = msg.getAttemptCount() + 1;
        if (attemptCount > MAX_GRAFT_ATTEMPTS)
        {
            logger.debug("abandoning GRAFT attempts as we've hit the retry limit of {}", MAX_GRAFT_ATTEMPTS);
            return;
        }

        // see if there are any other nodes in the announcements set, and send a GRAFT to one of the nodes
        // else, we know we have a broken tree, maybe the rejecting node could send along a candidate from it's own
        // branches (for the same tree root), one with the lowest loadEstimate
        //TODO: look into the 'announcements' set for a peer, or maybe just stick with this
        if (msg.getGraftAlternate() != null)
        {
            //NOTE: this is different from the thicket paper
            dispatcher.send(this, new GraftRequestMessage(msg.getTreeRoot(), msg.getClientId(), attemptCount, buildLoadEstimate()), msg.getGraftAlternate());
        }
    }

    public void registered(PeerSamplingService peerSamplingService)
    {
        for (InetAddress addr : peerSamplingService.getPeers())
            neighborUp(addr);
    }

    public void register(BroadcastClient client)
    {
        if (clients.containsKey(client.getClientId()))
            throw new IllegalStateException("cannot re-register broadcast client id: " + client.getClientId());
        clients.putIfAbsent(client.getClientId(), client);
    }

    public void neighborUp(InetAddress peer)
    {
        if (!backupPeers.contains(peer) && !peer.equals(getAddress()))
            backupPeers.add(peer);
    }

    public void neighborDown(InetAddress peer)
    {
        activePeers.remove(peer);
        for (CopyOnWriteArraySet<InetAddress> branches : activePeers.values())
            branches.remove(peer);
        loadEstimate = null;
        backupPeers.remove(peer);
    }

    public void updateClusterSize(int clusterSize)
    {
        if (clusterSize == 0)
            fanout = 1;
        fanout = (int)Math.ceil(Math.log(clusterSize));
    }

    /**
     * check to see if address is in parent thicket views, as we want to skip nodes we already know about via the peer sampling service.
     */
    boolean alreadyInView(InetAddress addr)
    {
        if (backupPeers.contains(addr))
            return true;

        if (activePeers.containsKey(addr))
            return true;

        for (CopyOnWriteArraySet<InetAddress> branches : activePeers.values())
        {
            if (branches.contains(addr))
                return true;
        }

        return false;
    }

    public InetAddress getAddress()
    {
        return config.getLocalAddr();
    }

    @VisibleForTesting
    List<InetAddress> getBackupPeers()
    {
        return ImmutableList.copyOf(backupPeers);
    }

    @VisibleForTesting
    // DO NOT USE OUTSIDE OF TESTING!!!
    void setBackupPeers(List<InetAddress> peers)
    {
        backupPeers.addAll(peers);
        fanout = backupPeers.size();
    }

    @VisibleForTesting
    int getFanout()
    {
        return fanout;
    }

    @VisibleForTesting
    GossipDispatcher getDispatcher()
    {
        return dispatcher;
    }

    private class SummaryTask implements Runnable
    {
        // not sure this is the best way to do this (one Runnable just submitting another)
        public void run()
        {
            publish(new Runnable()
            {
                public void run()
                {
                    doSummary();
                }
            });
        }
    }

    public IEndpointStateChangeSubscriber getStateChangeSubscriber()
    {
        return stateChangeSubscriber;
    }

    /**
     * an internal call to move processing to the GOSSIP stage
     */
    void publish(Runnable runnable)
    {
        StageManager.getStage(Stage.GOSSIP).submit(runnable);
    }

    /**
     * Listen for changes in membership service, and use that as the basis for peer selection during anti-entropy sessions.
     * We use this to get a feed of all known nodes in the cluster, and filter out the ones we already know about from the
     * peer sampling service. This gives us a richer anti-entropy reconciliation, helps the cluster converge faster, and heal
     * any broken parts of the broadcast tree.
     */
    private class AntiEntropyPeerListener implements IEndpointStateChangeSubscriber
    {
        /**
         * mapping of address to datacenter
         */
        final ConcurrentMap<InetAddress, String> nodes;

        private AntiEntropyPeerListener()
        {
            nodes = new ConcurrentHashMap<>();
            // TODO: might want a way to snag all the existing entries, if any
        }

        Collection<InetAddress> selectNodes(int maxCount)
        {
            List<InetAddress> selected = new ArrayList<>(maxCount);

            // TODO: get a better distribution of local DC vs. (possible) remote DC nodes
            Utils.selectMultipleRandom(nodes.keySet(), selected, maxCount);

            return selected;
        }

        /*
            IEndpointStateChangeSubscriber methods
         */

        public void onJoin(InetAddress endpoint, EndpointState epState)
        {
            // nop - wait until node is alive
        }

        public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
        {
            // nop
        }

        public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
        {
            if (alreadyInView(endpoint))
                return;
            if (state.equals(ApplicationState.DC))
                nodes.put(endpoint, value.value);
        }

        public void onAlive(InetAddress endpoint, EndpointState state)
        {
            if (alreadyInView(endpoint))
                return;
            VersionedValue vv = state.getApplicationState(ApplicationState.DC);
            if (vv != null)
                nodes.put(endpoint, vv.value);
        }

        public void onDead(InetAddress endpoint, EndpointState state)
        {
            nodes.remove(endpoint);
        }

        public void onRemove(InetAddress endpoint)
        {
            nodes.remove(endpoint);
        }

        public void onRestart(InetAddress endpoint, EndpointState state)
        {
            // TODO: maybe notify thicket that the peer bounced, and any open anti-entropy sessions are likely dead
        }
    }

    private class AnnouncementTimeoutProcessor implements Function<Pair<ExpiringMapEntry, CacheableObject<CopyOnWriteArrayList<InetAddress>>>, Object>
    {
        public Object apply(final Pair<ExpiringMapEntry, CacheableObject<CopyOnWriteArrayList<InetAddress>>> input)
        {
            publish(new Runnable()
            {
                public void run()
                {
                    handleExpiredAnnouncement(input.left.clientId, input.left.messageId, input.left.treeRoot, input.right.value);
                }
            });
            return null;
        }
    }

    private static class ExpiringMapEntry
    {
        final String clientId;
        final Object messageId;
        final InetAddress treeRoot;

        private ExpiringMapEntry(String clientId, Object messageId, InetAddress treeRoot)
        {
            this.treeRoot = treeRoot;
            this.messageId = messageId;
            this.clientId = clientId;
        }

        public int hashCode()
        {
            return 37 * clientId.hashCode() + messageId.hashCode() + treeRoot.hashCode();
        }
    }
}
