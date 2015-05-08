package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyService;
import org.apache.cassandra.gms2.gossip.peersampling.HPVConfig;
import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketConfig;
import org.apache.cassandra.gms2.membership.MembershipService;

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

    public GossipService(HPVConfig hpvConfig, ThicketConfig plumtreeConfig, GossipDispatcher dispatcher)
    {
        scheduledService = Executors.newSingleThreadScheduledExecutor();

        hyParViewService = new HyParViewService(hpvConfig, dispatcher);
        //TODO: figure out the FD to pass into init()
        hyParViewService.init(scheduledService, null);

        broadcastService = new ThicketBroadcastService(plumtreeConfig, dispatcher);
        broadcastService.init(scheduledService);
        // broadcast requires an underlying peer sampling service
        hyParViewService.register(broadcastService);

        antiEntropyService = new AntiEntropyService();
        antiEntropyService.init(scheduledService);
        // anti-entropy requires a peer sampling service largely to attempt to avoid peers the broadcast service is using (if that is possible)
        hyParViewService.register(antiEntropyService);

        MembershipService membershipService = new MembershipService(hpvConfig.getLocalAddr());
        // membership needs to receive messages from peers
        broadcastService.register(membershipService);
        // membership needs to participate in periodic anti-entropy sessions
        antiEntropyService.register(membershipService);

        membershipService.register(broadcastService.getStateChangeSubscriber());
    }

    public void register(BroadcastClient client)
    {
        broadcastService.register(client);
    }


}
