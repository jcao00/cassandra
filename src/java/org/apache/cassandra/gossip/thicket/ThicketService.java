package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.gossip.BroadcastService;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;

//TODO:JEB need link to paper!!
/**
 * An implementation of <a href="">Thicket: A Protocol for Maintaining Multiple Trees in a P2P Overlay</a>.
 * A thorough treatment is obviously in the paper, but sumamrized here. Thicket uses an underlying {@link PeerSamplingService}
 * to provide a view of the cluster from which it constructs spanning trees, upon which messages are broadcast gossip-style.
 * Thicket attempts to have each node act as an interior node in at most one of the n trees of the cluster; we will have a tree
 * per-node, so n is equal to the number of nodes in the cluster.
 *
 * Thicket maintains a set of active peers and a set of backup peers; initially all peers are in the backup set. The active set
 * maintains the collection of peers that messages are broadcast to (or received from), while the backup peers is used for populating
 * the active peers if it is not at capacity. The peers in the two sets are largely derived from the view provided by the PeerSamplingService.
 *
 * Messages:
 * - DATA - encapsulates the broadcast payload to be sent to the peers in the cluster.
 * - SUMMARY -
 * - GRAFT -
 * - GRAFT_RESPONSE -
 * - PRUNE -
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
 * The primary
 */
public class ThicketService implements BroadcastService, PeerSamplingServiceListener
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketService.class);

    /**
     * The maximum number of message ids to retain per rooted spanning tree.
     */
    private static final int MESSAGE_ID_RETAIN_COUNT = 8;

    private static final long MESSAGE_ID_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(40, TimeUnit.SECONDS);

    private static final int MISSING_MESSAGES_TIMER_DELAY_SECONSS = 10;

    private final InetAddress localAddress;
    private final ThicketMessageSender messageSender;
    private final ExecutorService executor;
    private final DebuggableScheduledThreadPoolExecutor scheduledTasks;
    private GossipMessageId.IdGenerator idGenerator;

    private boolean executing;

    /**
     * Mapping of a message originatorig's address to the downstream branch peers.
     *
     * "for each tree T rooted at R, here is set the downstream peers P"
     */
    private final Multimap<InetAddress, InetAddress> activePeers;
    private final Collection<InetAddress> backupPeers;

    private final Map<String, BroadcastServiceClient> clients;
    private ScheduledFuture<?> summarySender;

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
    private final Multimap<InetAddress, TimestampedMessageId> missingMessages;

    // TODO:JEB need a way to determine this size
    private int fanout;

    /**
     *
     * Note: should be logarithmic with the number of nodes in the system.
     */
    private int maxLoad;

    public ThicketService(InetAddress localAddress, ThicketMessageSender messageSender, ExecutorService executor, DebuggableScheduledThreadPoolExecutor scheduledTasks)
    {
        this.localAddress = localAddress;
        this.messageSender = messageSender;
        this.executor = executor;
        this.scheduledTasks = scheduledTasks;

        clients = new HashMap<>();
        receivedMessages = new LinkedList<>();
        messagesLedger = new LinkedList<>();
        missingMessages = HashMultimap.create();
        activePeers = HashMultimap.create();
        backupPeers = new LinkedList<>();
    }

    public void start(PeerSamplingService peerSamplingService, int epoch)
    {
        if (executing)
            return;
        idGenerator = new GossipMessageId.IdGenerator(epoch);
        backupPeers.addAll(peerSamplingService.getPeers());
        executing = true;
        summarySender = scheduledTasks.scheduleWithFixedDelay(new SummarySender(), 10, 1, TimeUnit.SECONDS);
    }

    public void broadcast(Object payload, BroadcastServiceClient client)
    {
        GossipMessageId messageId = idGenerator.generate();
        receivedMessages.add(new TimestampedMessageId(localAddress, client.getClientName(), messageId, System.nanoTime() + MESSAGE_ID_RETENTION_TIME));
        if (!activePeers.containsKey(localAddress))
            // TODO:JEB paper suggests using fanout as number of backup node to put into active view
            activePeers.putAll(localAddress, backupPeers);

        DataMessage msg = new DataMessage(localAddress, messageId, payload, localAddress, client.getClientName());
        for (InetAddress peer : activePeers.get(localAddress))
            messageSender.send(peer, msg);
    }

    /**
     * Add the message id to the recently seen list, as well as remove it from any missing messages list.
     */
    void recordMessage(InetAddress originator, BroadcastServiceClient client, GossipMessageId messageId)
    {
        TimestampedMessageId msgId = new TimestampedMessageId(originator, client.getClientName(), messageId, System.nanoTime() + MESSAGE_ID_RETENTION_TIME);
        receivedMessages.add(msgId);
        missingMessages.remove(originator, msgId);
    }

    public void receiveMessage(ThicketMessage message)
    {
        try
        {
            switch (message.getMessageType())
            {
                case DATA:      handleData((DataMessage)message); break;
                case SUMMARY:   handleSummary((SummaryMessage)message); break;
                case GRAFT:     handleGraft(message); break;
                case PRUNE:     handlePrune(message); break;
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
        BroadcastServiceClient client = clients.get(message.client);
        if (client == null)
        {
            logger.info("recieved broadcast message for unknown client component: " + message.client);
            return;
        }

        if (client.receive(message.payload))
        {
            //broadcast(message.sender, message.payload, message.originator, client);
        }
        else
        {
            // TODO:JEB we've seen the messageId already

        }
    }

    /**
     * Process an incoming SUMMARY message from a peer. The goal here is to ensure the tree is healthy (no broken branches,
     * entire tree is spanned), *not* data convergence. Data convergence can be piggy-backed here for convenience,
     * or via anti-entropy, but that's not the primary purpose of the SUMMARY flow.
     */
    void handleSummary(SummaryMessage msg)
    {
        Multimap<InetAddress, TimestampedMessageId> missingMessages = HashMultimap.create();
        for (TimestampedMessageId message : msg.receivedMessages)
        {
            // TODO:JEB optimize these data structures for fast lookups (lists will iterate the whole enchillada)
            if (!receivedMessages.contains(message) || !messagesLedger.contains(message))
            {
                // TODO:JEB add in SUMMARY sender's addr here so we can GRAFT to that node later
                missingMessages.put(message.originator, message);
            }
        }

        // TODO:JEB do we want to send a response back to the SUMMARY sender?

        if (missingMessages.isEmpty())
            return;

        // "store missing message ids in the 'announcements' field"
        this.missingMessages.putAll(missingMessages);

        // "for each tree t where a message is missing, start a repair timer"
        scheduledTasks.schedule(new MissingMessagesTimer(missingMessages), MISSING_MESSAGES_TIMER_DELAY_SECONSS, TimeUnit.SECONDS);
    }

    void handleGraft(ThicketMessage message)
    {

    }

    void handlePrune(ThicketMessage message)
    {

    }

    /**
     * Send a SUMMARY message containing all received message IDs to all peers in the backup set.
     */
    void sendSummary()
    {
        if (!executing)
            return;

        SummaryMessage message = new SummaryMessage(localAddress, idGenerator.generate(), receivedMessages);

        for (InetAddress peer : backupPeers)
            messageSender.send(peer, message);

        pruneMessageLedger(messagesLedger);
        messagesLedger.addAll(receivedMessages);
        receivedMessages.clear();
    }

    void pruneMessageLedger(List<TimestampedMessageId> messagesLedger)
    {
        // TODO:JEB perhaps we should bound the number of messages in the ledger, as well
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

    public void shutdown()
    {
        if (!executing)
            return;
        executing = false;
        summarySender.cancel(true);
        scheduledTasks.shutdown();
    }

    /*
        methods for PeerSamplingServiceListener
     */
    public void neighborUp(InetAddress peer, String datacenter)
    {
        activePeers.removeAll(localAddress);
        // TODO:JEB is there a better way to remove the peer from the values in the map???
        for (InetAddress addr : activePeers.keySet())
            activePeers.remove(addr, peer);
        backupPeers.add(peer);
    }

    public void neighborDown(InetAddress peer, String datacenter)
    {
        backupPeers.remove(peer);
    }

    void checkMissingMessages(Multimap<InetAddress, TimestampedMessageId> reportedMissing)
    {
        // target address {key} to which we want to add current node for the given tree roots {values}.
        // creating this structure to optimize the GRAFT message count we send
        Multimap<InetAddress, InetAddress> graftTargets = null;
        for (Map.Entry<InetAddress, Collection<TimestampedMessageId>> tree : reportedMissing.asMap().entrySet())
        {
            Collection<TimestampedMessageId> missingTreeMessages = missingMessages.get(tree.getKey());

            // we've received all the outstanding messages
            if (missingTreeMessages.isEmpty())
                continue;

            // check to see if the reported missing have been received - other peers could have sent us SUMAMRY messages
            // with either older or newer message ids
            Collection<TimestampedMessageId> msgs = tree.getValue();
            for (Iterator<TimestampedMessageId> iter = msgs.iterator(); iter.hasNext(); )
            {
                if (missingTreeMessages.contains(iter.next()))
                    iter.remove();
            }

            // we have received all of the outstanding messages this event was interested in
            if (msgs.isEmpty())
                continue;

            if (graftTargets == null)
                graftTargets = HashMultimap.create();
            // TODO:JEB select the target for the GRAFT message (rather than hacking in 'null')
            // should also add into the calculation of loadEstimate for further iterations
            graftTargets.put(null, tree.getKey());

            // TODO:JEB we've determined we want to graft, so clear out the missing messages field for the tree root
            // so no other (currently scheduled) timer goes off and tries to GRAFT for the same root
            missingMessages.removeAll(tree.getKey());


            // GRAFT_RESPONSE - sends along update for data converegnce (possibly), but because we know we'll be getting
            // future updates, allows us to invalidate/ignore any message ids received between the time we send the GRAFT request
            // and the time we recieved the the GRAFT_RESPONSE. (It would be great to send the outstanding missing message ids
            // in the GRAFT req, and have *some* data come back in the response.)

        }

        if (graftTargets == null)
            return;

        for (Map.Entry<InetAddress, Collection<InetAddress>> entry : graftTargets.asMap().entrySet())
        {
            messageSender.send(entry.getKey(), new GraftMessage(idGenerator.generate(), entry.getValue()));
        }
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
        private final Multimap<InetAddress, TimestampedMessageId> missingMessages;

        public MissingMessagesTimer(Multimap<InetAddress, TimestampedMessageId> missingMessages)
        {
            this.missingMessages = missingMessages;
        }

        public void run()
        {
            executor.execute(ThicketService.this::checkMissingMessages);
        }
    }

    /**
     * A simple struct to capture metadata about recently received messages from peers.
     */
    class TimestampedMessageId
    {
        final InetAddress originator;
        final String clientName;
        final GossipMessageId messageId;
        final long expirationTime;

        private TimestampedMessageId(InetAddress originator, String clientName, GossipMessageId messageId, long expirationTime)
        {
            this.originator = originator;
            this.clientName = clientName;
            this.messageId = messageId;
            this.expirationTime = expirationTime;
        }
    }
}
