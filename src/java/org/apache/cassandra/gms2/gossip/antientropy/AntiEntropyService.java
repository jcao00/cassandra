package org.apache.cassandra.gms2.gossip.antientropy;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AntiEntropyMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynAckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynMessage;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingServiceClient;
import org.apache.cassandra.gms2.membership.PeerSubscriber;

/**
 * A system for exchanging data in the push-pull, anti-entropy style. Similar to cassandra's initial
 * gossip system (a push-pull system based on the Scuttlebutt paper), a peer is selected random,
 * and a 'session' consisting of three messages is exchanged: SYN, ACK, SYNACK (similar to the
 * TCP three-way handshake). The service imposes no constraints on the message format/protocol
 * that {@code AntiEntropyClient}s must implement, but typical implementations would look like
 * the protocol described in the Scuttlebutt paper, or they exchange merkle trees and diffs, and so on.
 */
public class AntiEntropyService implements PeerSamplingServiceClient, GossipDispatcher.GossipReceiver<AntiEntropyMessage>
{
    private static final Logger logger = LoggerFactory.getLogger(AntiEntropyService.class);

    private final AntiEntropyConfig config;
    private final GossipDispatcher dispatcher;
    /**
     * The primary source for getting peers to exchange with.
     */
    private final PeerSubscriber peerSubscriber;

    /**
     * Nodes that we heard about form the peer sampling service
     * Use those nodes as a filter for those from the {@code peerSubscriber}.
     */
    private final Collection<InetAddress> peerSamplingNodes;

    /**
     * Known anti-entropy participants.
     */
    private final List<AntiEntropyClient> clients;

    /**
     * An index into the {@code clients} collection, to be used for a simple round robin mechanism.
     */
    private int currentClient;

    public AntiEntropyService(AntiEntropyConfig config, GossipDispatcher dispatcher, PeerSubscriber peerSubscriber)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        this.peerSubscriber = peerSubscriber;
        clients = new CopyOnWriteArrayList<>();
        peerSamplingNodes = new CopyOnWriteArrayList<>();
    }

    public void init(ScheduledExecutorService scheduledService)
    {
        scheduledService.scheduleAtFixedRate(new NextSessionStarter(), 30, 10, TimeUnit.SECONDS);
    }

    private class NextSessionStarter implements Runnable
    {
        public void run()
        {
            try
            {
                doNextSession();
            }
            catch (Exception e)
            {
                logger.error("failed to execute next anti-entropy session", e);
            }
        }
    }

    public void doNextSession()
    {
        AntiEntropyClient client;
        switch (clients.size())
        {
            case 0:
                return;
            case 1:
                client = clients.get(0);
                break;
            default:
                client = clients.get(currentClient % clients.size());
                currentClient++;
        }

        InetAddress antiEntropyPeer = selectPeer(peerSubscriber.getNodes(), peerSamplingNodes);
        if (antiEntropyPeer == null)
        {
            logger.info("no nodes to exchange anti-entropy data with");
            return;
        }

        Object o = client.preparePush();
        SynMessage msg = new SynMessage(client.getClientId(), o);
        dispatcher.send(this, msg, antiEntropyPeer);
    }

    InetAddress selectPeer(Set<InetAddress> clusterNodes, Collection<InetAddress> filter)
    {
        // TODO: determine if we should probabalistically go cross-DC
        // get the difference between the two peers
        // if no diffs, select a random node from the primary (either local or any remote DC per-probability)
        // if diffs, select random node from diffs (either local or any remote DC per-probability)
        return Utils.selectRandom(clusterNodes, filter.toArray(new InetAddress[0]));
    }

    public void handle(AntiEntropyMessage msg, InetAddress sender)
    {
        try
        {
            switch (msg.getMessageType())
            {
                case SYN:
                    handleSyn((SynMessage) msg, sender);
                    break;
                case ACK:
                    handleAck((AckMessage) msg, sender);
                    break;
                case SYN_ACK:
                    handleSynAck((SynAckMessage) msg, sender);
                    break;
                default:
                    throw new IllegalArgumentException("unknown anti-entropy message type " + msg.getMessageType().name());
            }
        }
        catch (Exception e)
        {
            logger.error("failed to handle anti-entropy message " + msg.getMessageType(), e);
        }
    }

    public InetAddress getAddress()
    {
        return config.getAddress();
    }

    AntiEntropyClient getClient(String clientId)
    {
        for (AntiEntropyClient client : clients)
        {
            if (client.getClientId().equals(clientId))
                return client;
        }
        return null;
    }

    void handleSyn(SynMessage msg, InetAddress sender) throws IOException
    {
        AntiEntropyClient client = getClient(msg.getClientId());
        if (client == null)
        {
            logger.warn("received anti-entropy SYN for an unknown client: " + msg.getClientId());
            return;
        }

        Object pullData = client.processPush(msg.getPushData());
        AckMessage ackMSg = new AckMessage(client.getClientId(), pullData);
        dispatcher.send(this, ackMSg, sender);
    }

    void handleAck(AckMessage msg, InetAddress sender) throws IOException
    {
        AntiEntropyClient client = getClient(msg.getClientId());
        if (client == null)
        {
            logger.warn("received anti-entropy ACK for an unknown client: " + msg.getClientId());
            return;
        }

        Object pushPullData = client.processPull(msg.getPullData());
        if (pushPullData == null)
            return;
        SynAckMessage synAckMessage = new SynAckMessage(client.getClientId(), pushPullData);
        dispatcher.send(this, synAckMessage, sender);
    }

    void handleSynAck(SynAckMessage msg, InetAddress sender) throws IOException
    {
        AntiEntropyClient client = getClient(msg.getClientId());
        if (client == null)
        {
            logger.warn("received anti-entropy SYNACK for an unknown client: " + msg.getClientId());
            return;
        }

        client.processPushPull(msg.getPushPullData());
    }

    public void register(AntiEntropyClient client)
    {
        clients.add(client);
    }

    /*
        methods for PeerSamplingServiceClient
     */

    public void registered(PeerSamplingService peerSamplingService)
    {
        // nop
    }

    public void neighborUp(InetAddress peer)
    {
        peerSamplingNodes.add(peer);
    }

    public void neighborDown(InetAddress peer)
    {
        peerSamplingNodes.remove(peer);
    }
}
