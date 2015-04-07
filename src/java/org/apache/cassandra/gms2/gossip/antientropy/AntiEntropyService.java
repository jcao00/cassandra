package org.apache.cassandra.gms2.gossip.antientropy;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.cassandra.gms2.gossip.antientropy.messages.AckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AntiEntropyMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynAckMessage;
import org.apache.cassandra.gms2.gossip.antientropy.messages.SynMessage;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;

public class AntiEntropyService
{
    /**
     * The primary source for getting peers.
     */
    private final PeerSamplingService primary;

    /**
     * A source that yields peers to not prefer. Those peers may be used if no other peers are available,
     * as in the case of small clusters or network partitions.
     */
    private final PeerSamplingService filter;

    private final Collection<AntiEntropyClient> clients;

    public AntiEntropyService(PeerSamplingService primary, PeerSamplingService filter)
    {
        this.primary = primary;
        this.filter = filter;
        clients = new LinkedList<>();
    }

    public void execute()
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
}
