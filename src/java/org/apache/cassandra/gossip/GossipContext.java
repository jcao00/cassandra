package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gossip.hyparview.HyParViewService;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class GossipContext
{
    public final HyParViewService hyparviewService;

    public GossipContext()
    {
        InetAddress localAddress = FBUtilities.getBroadcastAddress();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        String datacenter = snitch.getDatacenter(localAddress);

        // TODO:JEB figure out what to do with active random walk length ....
        hyparviewService = new HyParViewService(localAddress, datacenter, DatabaseDescriptor.getSeedProvider(), new StandardMessageSender(),
                                               StageManager.getStage(Stage.GOSSIP), ScheduledExecutors.scheduledTasks, 3);
        Gossiper.instance.register(hyparviewService.endpointStateSubscriber);
    }

    public void start(int epoch)
    {
        hyparviewService.start(epoch);
    }

    public void shutdown()
    {
        hyparviewService.shutdown();
    }
}
