package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyService;
import org.apache.cassandra.gms2.gossip.peersampling.HPVConfig;
import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketConfig;
import org.apache.cassandra.gms2.membership.MembershipService;

public class GossipService
{
    public final HyParViewService hyParViewService;
    public final ThicketBroadcastService plumtreeService;

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

        plumtreeService = new ThicketBroadcastService(plumtreeConfig, dispatcher);
        plumtreeService.init(scheduledService);
        hyParViewService.register(plumtreeService);

        MembershipService membershipService = new MembershipService(hpvConfig.getLocalAddr());
        AntiEntropyService antiEntropyService = new AntiEntropyService(membershipService, hyParViewService);
        membershipService.init(antiEntropyService);
    }
}
