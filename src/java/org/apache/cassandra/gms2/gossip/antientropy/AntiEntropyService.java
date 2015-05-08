package org.apache.cassandra.gms2.gossip.antientropy;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AntiEntropyMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynAckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynMessage;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingServiceClient;
import org.apache.cassandra.gms2.membership.PeerSubscriber;

public class AntiEntropyService implements PeerSamplingServiceClient
{
    /**
     * The primary source for getting peers.
     */
    private final PeerSamplingService peerSamplingService;

    private final PeerSubscriber peerSubscriber;

    private final Collection<AntiEntropyClient> clients;

    public AntiEntropyService(PeerSamplingService peerSamplingService, PeerSubscriber peerSubscriber)
    {
        this.peerSamplingService = peerSamplingService;
        this.peerSubscriber = peerSubscriber;
        clients = new LinkedList<>();
    }

    public void init(ScheduledExecutorService scheduledService)
    {

    }

    public void doNextSession()
    {
        // 1. determine peer(s) to exchange data with
        // determine if we should probabalistically go cross-DC
        // get the difference between the two peers
        // if no diffs, select a random node from the primary (either local or any remote DC per-probability)
        // if diffs, select random node from diffs (either local or any remote DC per-probability)

        // 2. get data to exchange
        // for now, just ship all clients' data together in one message. in the future, might want to
        // send each client's anti-entropy data separately in different A-E sessions, but lazy for now.
    }

    public void handle(AntiEntropyMessage msg, InetSocketAddress sender)
    {
        switch(msg.getMessageType())
        {
            case SYN: handleSyn((SynMessage)msg, sender); break;
            case ACK: handleAck((AckMessage)msg, sender); break;
            case SYN_ACK: handleSynAck((SynAckMessage)msg, sender); break;
            default:
                throw new IllegalArgumentException("unknown anti-entropy message type " + msg.getMessageType().name());
        }
    }

    void handleSyn(SynMessage msg, InetSocketAddress sender)
    {

    }

    void handleAck(AckMessage msg, InetSocketAddress sender)
    {

    }

    void handleSynAck(SynAckMessage msg, InetSocketAddress sender)
    {

    }

    public void register(AntiEntropyClient client)
    {
        clients.add(client);
    }


    /**
     * check to see if address is in parent thicket views, as we want to skip nodes we already know about via the peer sampling service.
     */
    boolean alreadyInView(InetAddress addr)
    {
//        return alreadyInView(addr, activePeers, backupPeers);
        return true;
    }

//    @VisibleForTesting
//    boolean alreadyInView(InetAddress addr, ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers, Collection<InetAddress> backupPeers)
//    {
//        if (backupPeers.contains(addr))
//            return true;
//
//        if (activePeers.containsKey(addr))
//            return true;
//
//        for (CopyOnWriteArraySet<InetAddress> branches : activePeers.values())
//        {
//            if (branches.contains(addr))
//                return true;
//        }
//
//        return false;
//    }

    /*
        methods for PeerSamplingServiceClient
     */

    public void registered(PeerSamplingService peerSamplingService)
    {

    }

    public void neighborUp(InetAddress peer)
    {

    }

    public void neighborDown(InetAddress peer)
    {

    }
}
