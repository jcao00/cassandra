package org.apache.cassandra.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.hyparview.PennStationDispatcher;
import org.apache.cassandra.gossip.hyparview.SimulationMessageSender;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.SeedProvider;

// If keeping with famous NYC transportation hubs for naming the gossip simualtors,
// I can't bring myself to name this class after the NYC Port Authority.
public class TimesSquareDispacher extends PennStationDispatcher
{
    private static final Logger logger = LoggerFactory.getLogger(TimesSquareDispacher.class);
    List<NodeContext> contexts;

    public TimesSquareDispacher(boolean verbose)
    {
        super(verbose);
    }

    public void addPeer(InetAddress addr, String datacenter)
    {
        SimulationMessageSender messageSender = new SimulationMessageSender(addr, this);
        peers.put(addr, new BroadcastNodeContext(addr, datacenter, seedProvider, messageSender));
    }

    public void sendMessage(InetAddress source, InetAddress destination, ThicketMessage message)
    {
        if (!peers.containsKey(destination))
        {
            if (seedProvider.getSeeds().contains(destination))
                return;
            logger.info(String.format("%s sending [%s] to non-existent node %s - might be ok", source, message, destination));
            return;
        }

        totalMessagesSent.incrementAndGet();
        ((BroadcastNodeContext)peers.get(destination)).send(message, 0);
    }

    public BroadcastNodeContext selectRandom()
    {
        if (contexts == null)
        {
            contexts = new ArrayList<>(peers.values());
            Collections.shuffle(contexts, random);
        }

        return (BroadcastNodeContext)contexts.get(random.nextInt(contexts.size()));
    }

    BroadcastNodeContext getContext(InetAddress addr)
    {
        return (BroadcastNodeContext)peers.get(addr);
    }

    protected static class SimpleClient implements BroadcastServiceClient<String>
    {
        private static final Serializer SERIALIZER = new Serializer();
        final Set<String> received = new HashSet<>();

        public String getClientName()
        {
            return "simple-client";
        }

        public boolean receive(String payload)
        {
            if (received.contains(payload))
                return false;
            received.add(payload.toString());
            return true;
        }

        public IVersionedSerializer<String> getSerializer()
        {
            return SERIALIZER;
        }

        private static class Serializer implements IVersionedSerializer<String>
        {
            public void serialize(String s, DataOutputPlus out, int version) throws IOException
            {
                out.writeUTF(s);
            }

            public long serializedSize(String s, int version)
            {
                return TypeSizes.sizeof(s);
            }

            public String deserialize(DataInputPlus in, int version) throws IOException
            {
                return in.readUTF();
            }
        }
    }

    protected class BroadcastNodeContext extends NodeContext
    {
        final ThicketService thicketService;
        final BroadcastServiceClient<String> client;

        BroadcastNodeContext(InetAddress addr, String datacenter, SeedProvider seedProvider, SimulationMessageSender messageSender)
        {
            super(addr, datacenter, seedProvider, messageSender);
            BroadcastMessageSender broadcastMessageSender = new BroadcastMessageSender(addr, TimesSquareDispacher.this);
            // reuse the schedule from the parent class, assuming hyparview and thicket execute in the thread (for each node)
            thicketService = new ThicketService(addr, broadcastMessageSender, scheduler, scheduler);
            hpvService.register(thicketService);
            thicketService.start(hpvService, Math.abs(random.nextInt()), 0, 5, 0, 10);
            client = new SimpleClient();
            thicketService.register(client);
        }

        void send(ThicketMessage message, long delay)
        {
//            if (verbose)
//                logger.info(String.format("scheduling [%s] to %s", message.toString(), hpvService.getLocalAddress()));
            currentlyProcessingCount.incrementAndGet();
            scheduler.schedule(() -> deliver(message), delay, TimeUnit.MILLISECONDS);
        }

        void deliver(ThicketMessage message)
        {
            if (verbose)
                logger.info(String.format("delivering [%s] to %s", message.toString(), thicketService.getLocalAddress()));
            thicketService.receiveMessage(message);
            currentlyProcessingCount.decrementAndGet();
        }

        public void shutdown()
        {
            super.shutdown();
            thicketService.shutdown();
        }
    }
}
