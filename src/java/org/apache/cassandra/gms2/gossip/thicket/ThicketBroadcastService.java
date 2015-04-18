package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
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
     * Collection of peers from which to draw when contacted by a new {sender/tree root} combination.
     * These entries would become the target peers for broadcasting.
     */
    // TODO: should peer sampling service be the only source for backups? especially in the case of anti-entropy --
    // plus we want dc-weighting for anti-entropy
    private final Collection<InetAddress> backupPeers;

    // not sure I like having yet another data structure hanging about, solely for the load estimates
    // however, we do want to keep around load estimates after peers come and go from the active and backup lists (I think...)
    private final ConcurrentMap<InetAddress, Float> loadEstimates;

    /**
     * The load estimate of this node; maintained in it's own field for ease of lookup.
     */
    private float loadEstimate;

    private final ConcurrentMap<String, BroadcastClient> clients;

    private PeerSamplingService peerSamplingService;

    /**
     * The maximum number of peers to add as downstream tree nodes (in an activePeers set).
     */
    private int fanout = 1;


    public ThicketBroadcastService(ThicketConfig config, GossipDispatcher dispatcher)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        activePeers = new ConcurrentHashMap<>();
        loadEstimates = new ConcurrentHashMap<>();

        //TODO: should this be a set instead? not sure if we care about FIFO properties... (we don't)
        backupPeers = new CopyOnWriteArrayList<>();
        clients = new ConcurrentHashMap<>();
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
            //TODO: pass along better load estimate
            ThicketDataMessage dataMessage = new ThicketDataMessage(localAddr, clientId, messageId, message, loadEstimate);

            for (InetAddress addr : targets)
                dispatcher.send(this, dataMessage, addr);
        }
        else
        {
            // TODO: possibly queue - unless cluster of one node (as in local testing)
        }
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
        //TODO: capture loadEst and other metrics from the message (that thicket requires us to keep around)
        // that may require a change to the activePeers data structure
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
            ThicketDataMessage dataMessage = new ThicketDataMessage(msg, loadEstimate);
            for (InetAddress addr : targets)
            {
                if (!addr.equals(sender))
                    dispatcher.send(this, dataMessage, addr);
            }
        }
        else
        {
            // TODO: as per section 4.4, "Tree Reconfiguration", check to see if redundant msg is already in announcements from a different node
            removeActivePeer(msg.getTreeRoot(), sender);
            dispatcher.send(this, new PruneMessage(msg.getTreeRoot(), loadEstimate), sender);
        }
    }

    void removeActivePeer(InetAddress treeRoot, InetAddress toRemove)
    {
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

    void handleSummary(SummaryMessage msg, InetAddress sender)
    {
        BroadcastClient client = clients.get(msg.getClientId());
        if (client == null)
        {
            logger.warn("received a summary request for an unknown client component: {}, ignoring", msg.getClientId());
            return;
        }

        client.receiveSummary(msg.getSummary());
        //TODO: implement rest of me
    }

    void handleGraftRequest(GraftRequestMessage msg, InetAddress sender)
    {
        // TODO: determine if we should accept this graft
        State state;
        if (true)
            state = State.ACCEPT;
        else
            state = State.REJECT;

        dispatcher.send(this, new GraftResponseMessage(msg.getTreeRoot(), state, loadEstimate), sender);
    }

    void handleGraftResponse(GraftResponseMessage msg, InetAddress sender)
    {
        // we've already done the optimistic work in the summary handling, so just process the reject
        if (msg.getState().equals(State.ACCEPT))
            return;
        //TODO: handle the reject case
    }

    void doSummary()
    {
        // TODO: might want to limit the number of open summary sessions



    }

    public void registered(PeerSamplingService peerSamplingService)
    {
        this.peerSamplingService = peerSamplingService;
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

        backupPeers.remove(peer);
    }

    public void updateClusterSize(int clusterSize)
    {
        if (clusterSize == 0)
            fanout = 1;
        fanout = (int)Math.ceil(Math.log(clusterSize));
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
        public void run()
        {
            doSummary();
        }
    }
}
