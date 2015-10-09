package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
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
 *
 */
public class ThicketService implements BroadcastService, PeerSamplingServiceListener
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketService.class);

    /**
     * The maximum number of message ids to retain per rooted spanning tree.
     */
    private static final int MESSAGE_ID_RETAIN_COUNT = 8;

    private static final long MESSAGE_ID_RETENTION_TIME = TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS);

    private final InetAddress localAddress;
    private final ThicketMessageSender messageSender;
    private final ExecutorService executor;
    private final DebuggableScheduledThreadPoolExecutor scheduledTasks;

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
    private final Map<InetAddress, LinkedList<TimestampedMessageId>> receivedMessages;

    public ThicketService(InetAddress localAddress, ThicketMessageSender messageSender, ExecutorService executor, DebuggableScheduledThreadPoolExecutor scheduledTasks)
    {
        this.localAddress = localAddress;
        this.messageSender = messageSender;
        this.executor = executor;
        this.scheduledTasks = scheduledTasks;

        clients = new HashMap<>();
        receivedMessages = new HashMap<>();
        activePeers = HashMultimap.create();
        backupPeers = new LinkedList<>();
    }

    public void start()
    {
        if (executing)
            return;
        executing = true;
        summarySender = scheduledTasks.scheduleWithFixedDelay(new SummarySender(), 10, 1, TimeUnit.SECONDS);
    }

    public void broadcast(Object messageId, Object payload)
    {
        recordMessageId(localAddress, messageId);
        broadcast(localAddress, messageId, payload, localAddress);
    }

    void recordMessageId(InetAddress address, Object messageId)
    {
        LinkedList<TimestampedMessageId> ids = receivedMessages.get(address);
        if (ids == null)
        {
            ids = new LinkedList<>();
            receivedMessages.put(address, ids);
        }
        ids.add(new TimestampedMessageId(messageId, System.nanoTime()));
    }

    void broadcast(InetAddress sender, Object messageId, Object payload, InetAddress originator)
    {
        // TODO:JEB - this is not correct, need to account for if this node already interior somewhere else
        if (!activePeers.containsKey(originator))
        {
            activePeers.putAll(originator, backupPeers);
        }

        DataMessage msg = new DataMessage(localAddress, messageId, payload, originator);
        for (InetAddress peer : activePeers.get(originator))
            if (!peer.equals(sender))
                messageSender.send(peer, msg);
    }

    public void receiveMessage(ThicketMessage message)
    {
        try
        {
            switch (message.getMessageType())
            {
                case DATA:      handleData((DataMessage)message); break;
                case SUMMARY:   handleSummary(message); break;
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

    private void handleData(DataMessage message)
    {
        // forward along to our peers
        broadcast(message.sender, message.messageId, message.payload, message.originator);

        // TODO:JEB now process message

    }

    private void handleSummary(ThicketMessage message)
    {

    }

    private void handleGraft(ThicketMessage message)
    {

    }

    private void handlePrune(ThicketMessage message)
    {

    }

    /**
     * Send a SUMMARY message containing all received message IDs to all peers in the backup set.
     */
    void sendSummary()
    {
        if (!executing)
            return;

        SummaryMessage message = new SummaryMessage(localAddress);
        // TODO:JEB add message IDs - iterate through

        for (InetAddress peer : backupPeers)
            messageSender.send(peer, message);
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

    private class SummarySender implements Runnable
    {
        public void run()
        {
            executor.execute(ThicketService.this::sendSummary);
        }
    }

    private class TimestampedMessageId
    {
        final Object messageId;
        final long expirationTime;

        private TimestampedMessageId(Object messageId, long expirationTime)
        {
            this.messageId = messageId;
            this.expirationTime = expirationTime;
        }
    }
}
