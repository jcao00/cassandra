package org.apache.cassandra.gms2.gossip.thicket;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;

/**
 * An implementation of the
 * <a html="http://asc.di.fct.unl.pt/~jleitao/pdf/srds10-mario.pdf">
 * Thicket: A Protocol for Building and Maintaining Multiple Trees in a P2P Overlay</a> paper.
 */
public class ThicketBroadcastService implements GossipBroadcaster
{
    private final ThicketConfig config;
    private final GossipDispatcher dispatcher;

    private final List<InetAddress> eagerPushPeers;
    private final List<InetAddress> lazyPushPeers;
    private PeerSamplingService peerSamplingService;

    public ThicketBroadcastService(ThicketConfig config, GossipDispatcher dispatcher)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        eagerPushPeers = new CopyOnWriteArrayList<>();
        lazyPushPeers = new CopyOnWriteArrayList<>();
    }

    public void init(ScheduledExecutorService scheduledService)
    {
        // TODO: set up background timer events
    }

    public void handle(ThicketMessage msg, InetAddress sender)
    {

    }

    public void registered(PeerSamplingService peerSamplingService)
    {
        this.peerSamplingService = peerSamplingService;
        eagerPushPeers.addAll(peerSamplingService.getPeers());
    }

    public void neighborUp(InetAddress peer)
    {

    }

    public void neighborDown(InetAddress peer)
    {

    }
}
