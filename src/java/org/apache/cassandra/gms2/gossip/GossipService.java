package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms2.gossip.peersampling.HPVConfig;
import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;

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
        hyParViewService.init(scheduledService);

        plumtreeService = new ThicketBroadcastService(plumtreeConfig, dispatcher);
        plumtreeService.init(scheduledService);
        hyParViewService.register(plumtreeService);

    }
}
