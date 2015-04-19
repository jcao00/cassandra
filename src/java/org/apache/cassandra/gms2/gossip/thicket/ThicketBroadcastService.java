package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.gms2.gossip.thicket.messages.GraftResponseMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.GraftResponseMessage.State;
import org.apache.cassandra.gms2.gossip.thicket.messages.PruneMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.SummaryMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketDataMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;

/**
 * An implementation of the
 * <a html="http://asc.di.fct.unl.pt/~jleitao/pdf/srds10-mario.pdf">
 * Thicket: A Protocol for Building and Maintaining Multiple Trees in a P2P Overlay</a> paper.
 */
public class ThicketBroadcastService<M extends ThicketMessage> implements GossipBroadcaster, GossipDispatcher.GossipReceiver<M>
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketBroadcastService.class);
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

        //TODO: should this be a set instead? not sure if we care about FIFO properties... (we don't)
        backupPeers = new CopyOnWriteArrayList<>();
        clients = new ConcurrentHashMap<>();
        stateChangeSubscriber = new AntiEntropyPeerListener();
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
        }
        else
        {
            // TODO: possibly queue - unless cluster of one node (as in local testing)
        }
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
                targets.add(sender);
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
                case GRAFT_RESPONSE:
                    handleGraftResponse((GraftResponseMessage) msg, sender);
                    break;
                default:
                    throw new IllegalArgumentException("unknown/unhandled thicket message type: " + msg.getMessageType());
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to handle thicket message", e);
        }
        finally
        {
            // capture the load estimate, as per the thicket paper
            loadEstimates.put(sender, msg.getLoadEstimate());
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

        //TODO: check if there are any open timers waiting for this message; if so, cancel them, and remove from 'announcements'

        boolean wasFresh = client.receiveBroadcast(msg.getMessageId(), msg.getMessage());
        if (wasFresh)
        {
            Collection<InetAddress> targets = getTargets(sender, msg.getTreeRoot());
            ThicketDataMessage dataMessage = new ThicketDataMessage(msg, buildLoadEstimate());
            for (InetAddress addr : targets)
            {
                if (!addr.equals(sender))
                    dispatcher.send(this, dataMessage, addr);
            }
        }
        else
        {
            //TODO: what should we do when tree root is the same as the sender?
            // it's possible that treeRoot broadcasted to one node (A), then paused (for whatever reason) -
            // and during that pause node A sent the message to B (due to various graft/prune mechanics),
            // and B is a direct target of the treeRoot. Then the treeRoot recovers, and broadcasts to B -
            // B will see the message is a dupe, but should it really prune the treeRoot from it's own tree?

            // TODO: as per section 4.4, "Tree Reconfiguration", check to see if redundant msg is already in announcements from a different node
            removeActivePeer(msg.getTreeRoot(), sender);
            dispatcher.send(this, new PruneMessage(msg.getTreeRoot(), buildLoadEstimate()), sender);
        }
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

    void doSummary()
    {
        Collection<InetAddress> destinations = summaryDestinations();

        // TODO: might want to limit the number of open summary sessions, as well as open sessions per peer (optimizations, i guess)

        // should we do a summary for *all* registered clients, ot just one? (to ease local and remote computational burden)
        // for now, I'm just shipping out for all of them - so yeah, a total of (numberOfClients x destinations) messages
        for (BroadcastClient client : clients.values())
        {
            //TODO: what tree root to use here?!?!?!?!?!  double check if we even need it for summary messaging
            SummaryMessage msg = new SummaryMessage(null, client.getClientId(), client.prepareSummary(), buildLoadEstimate());
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
        return peers;
    }

    void handleSummary(SummaryMessage msg, InetAddress sender)
    {
        BroadcastClient client = clients.get(msg.getClientId());
        if (client == null)
        {
            logger.warn("received a summary request for an unknown client component: {}, ignoring", msg.getClientId());
            return;
        }

        client.receiveSummary(msg.getSummary());
        //TODO: implement rest of me - should receiveSummary() return set of missing msgIds? should client be responsible for timers, etc??
        // would prefer to keep the timer baggage out of the clients
    }

    void handleGraftRequest(GraftRequestMessage msg, InetAddress sender)
    {
        // TODO: determine if we should accept this graft
        State state;
        if (true)
            state = State.ACCEPT;
        else
            state = State.REJECT;

        dispatcher.send(this, new GraftResponseMessage(msg.getTreeRoot(), state, buildLoadEstimate()), sender);
    }

    void handleGraftResponse(GraftResponseMessage msg, InetAddress sender)
    {
        // we've already done the optimistic work in the summary handling, so just process the reject
        if (msg.getState().equals(State.ACCEPT))
            return;
        //TODO: handle the reject case
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

        // TODO: find more efficient way of testing for existence in the active peers
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
        //TODO: publish onto the same thread that handles all of the thicket processing
        public void run()
        {
            doSummary();
        }
    }

    public IEndpointStateChangeSubscriber getStateChangeSubscriber()
    {
        return stateChangeSubscriber;
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
            // TODO: maybe notify thicket that the peer bounced, and any open anti-entopry sessions are likely dead
        }
    }
}
