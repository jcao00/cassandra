package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyClient;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyConfig;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyService;
import org.apache.cassandra.gms2.gossip.peersampling.HPVConfig;
import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketConfig;
import org.apache.cassandra.gms2.membership.MembershipService;
import org.apache.cassandra.gms2.membership.PeerSubscriber;

public class GossipService
{
    public final HyParViewService hyParViewService;
    public final ThicketBroadcastService broadcastService;
    public final AntiEntropyService antiEntropyService;

    /**
     * To be used for lightweight, gossip-related event scheduling. For example,
     * heartbeat incrementing and publishing, starting the next round of HyParView
     * shuffle, and so on.
     */
    public final ScheduledExecutorService scheduledService;

    public GossipService(HPVConfig hpvConfig, ThicketConfig plumtreeConfig, GossipDispatcher dispatcher, AntiEntropyConfig antiEntropyConfig)
    {
        scheduledService = Executors.newSingleThreadScheduledExecutor();

        hyParViewService = new HyParViewService(hpvConfig, dispatcher);
        //TODO: figure out the FD to pass into init()
        hyParViewService.init(scheduledService, null);

        PeerSubscriber peerSubscriber = new PeerSubscriber();

        broadcastService = new ThicketBroadcastService(plumtreeConfig, dispatcher, peerSubscriber);
        broadcastService.init(scheduledService);
        // broadcast requires an underlying peer sampling service
        hyParViewService.register(broadcastService);

        antiEntropyService = new AntiEntropyService(antiEntropyConfig, dispatcher, peerSubscriber);
        antiEntropyService.init(scheduledService);
        // anti-entropy requires a peer sampling service largely to attempt to avoid peers the broadcast service is using (if that is possible)
        hyParViewService.register(antiEntropyService);

        MembershipService membershipService = new MembershipService(hpvConfig.getLocalAddr());
        membershipService.register(peerSubscriber);

        // membership needs to receive messages from peers
        broadcastService.register(membershipService);
        // membership needs to participate in periodic anti-entropy sessions
        antiEntropyService.register(membershipService);

        // TODO: get all membership data from peers - not exactly sure where that happens
    }

    public void register(BroadcastClient client)
    {
        broadcastService.register(client);
    }

    public void register(AntiEntropyClient client)
    {
        antiEntropyService.register(client);
    }
}
