package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gossip.hyparview.HyParViewMessageSender;
import org.apache.cassandra.gossip.hyparview.HyParViewService;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class GossipContext
{
    /**
     * Until entire new gossip stack is ready, only start up when explicitly requested.
     */
    private final boolean enabled;

    public final HyParViewService hyparviewService;

    public GossipContext()
    {
        enabled = DatabaseDescriptor.newGossipEnabled();
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String datacenter = snitch.getDatacenter(localAddress);

        hyparviewService = new HyParViewService(localAddress, datacenter, DatabaseDescriptor.getSeedProvider(), new HyParViewMessageSender(),
                                               StageManager.getStage(Stage.GOSSIP), ScheduledExecutors.scheduledTasks);

        if (enabled)
            Gossiper.instance.register(hyparviewService.endpointStateSubscriber);
    }

    public void start(int epoch)
    {
        if (!enabled)
            return;
        hyparviewService.start(epoch);
    }

    public void shutdown()
    {
        if (!enabled)
            return;
        hyparviewService.shutdown();
    }
}
