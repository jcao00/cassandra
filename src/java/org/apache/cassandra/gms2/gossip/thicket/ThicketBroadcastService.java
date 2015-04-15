package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class ThicketBroadcastService implements GossipBroadcaster
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
    // TODO could use a set instead of a list, but then lose FIFO properties (does that matter?), although Utils
    // TODO: should peer sampling service be the only source for backups? especially in the case of anti-entropy --
    // plus we want dc-weighting for anti-entropy
    private final List<InetAddress> backupPeers;

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
        Collection<InetAddress> targets = getTargets(localAddr, localAddr);

        if (!targets.isEmpty())
        {
            ThicketDataMessage dataMessage = new ThicketDataMessage(localAddr, clientId, messageId, message, new byte[0]);

            for (InetAddress addr : targets)
                dispatcher.send(this, dataMessage, addr);
        }
        else
        {
            // TODO: possibly queue
        }
    }

    Collection<InetAddress> getTargets(InetAddress sender, InetAddress treeRoot)
    {
        CopyOnWriteArraySet<InetAddress> targets = activePeers.get(treeRoot);
        if (targets != null)
        {
            //TODO: confirm this is totally legit
            if (!targets.contains(sender))
                targets.add(sender);
            return targets;
        }

        targets = new CopyOnWriteArraySet<>(buildActivePeers(sender, treeRoot));
        CopyOnWriteArraySet<InetAddress> existing = activePeers.putIfAbsent(treeRoot, targets);
        if (existing != null)
            return existing;
        return targets;
    }

    final List<InetAddress> buildActivePeers(InetAddress sender, InetAddress treeRoot)
    {
        // if this node is already interior to some other tree, the activePeers for the new tree (for this node)
        // will only contain the sender (so this node acts as a leaf)
        if (isInterior())
        {
            List<InetAddress> peers = new ArrayList<>(1);
            peers.add(sender);
            return peers;
        }

        List<InetAddress> peers = new ArrayList<>(fanout);
        peers.add(sender);

        List<InetAddress> filter = new ArrayList<>(1);
        peers.add(treeRoot);

        Utils.selectMultipleRandom(backupPeers, peers, filter, fanout);
        return peers;
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
    }

    void handleDataMessage(ThicketDataMessage msg, InetAddress sender) throws IOException
    {
        BroadcastClient client = clients.get(msg.getClientId());
        if (client == null)
        {
            logger.warn("received a message for unknown client component: {}; ignoring message", msg.getClientId());
            return;
        }

        //TODO: check if there are any open timers waiting for this message; if so, cancel them, and remove from 'announcements'

        boolean wasFresh = client.receiveBroadcast(msg.getMessageId(), msg.getMessage());
        if (wasFresh)
        {
            Collection<InetAddress> targets = getTargets(sender, msg.getTreeRoot());
            ThicketDataMessage dataMessage = new ThicketDataMessage(msg, new byte[0]);
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
            dispatcher.send(this, new PruneMessage(msg.getTreeRoot(), new byte[0]), sender);
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

        dispatcher.send(this, new GraftResponseMessage(msg.getTreeRoot(), state, new byte[0]), sender);
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
        if (!backupPeers.contains(peer))
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

    private class SummaryTask implements Runnable
    {
        public void run()
        {
            doSummary();
        }
    }
}
