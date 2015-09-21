package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.SeedProvider;

public class PennStationDispatcher
{
    private static final Logger logger = LoggerFactory.getLogger(PennStationDispatcher.class);
    private static final int SEED = 1723618723;

    private final Map<InetAddress, NodeContext> peers;
    private final SeedProvider seedProvider;

    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Random random;

    public PennStationDispatcher(List<InetAddress> seeds)
    {
        this.random = new Random(SEED);
        peers = new HashMap<>();
        seedProvider = new SimulationSeedProvider(seeds);
    }

    public void addPeer(InetAddress addr, String datacenter)
    {
        SimulationMessageSender messageSender = new SimulationMessageSender(addr, this);
        peers.put(addr, new NodeContext(addr, datacenter, seedProvider, messageSender));
    }

    public HyParViewService getPeerService(InetAddress addr)
    {
        if (!peers.containsKey(addr))
            throw new IllegalArgumentException("Unknown addr: " + addr);
        return peers.get(addr).hpvService;
    }

    public void sendMessage(InetAddress source, InetAddress destination, HyParViewMessage message)
    {
        if (!peers.containsKey(destination))
        {
            logger.debug(String.format("%s sending message to non-existent node %s - might be ok", source, destination));
            return;
        }

        peers.get(destination).send(message, 0);
    }

    public void awaitQuiesence()
    {
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
//        paused.set(true);

        for (NodeContext context : peers.values())
        {
            try
            {
                context.awaitQuiesence();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

//        paused.set(false);
    }

    public void shutdown()
    {
        for (NodeContext context : peers.values())
        {
            context.scheduler.shutdownNow();
        }
    }

    public void dumpCurrentState()
    {
        StringBuffer sb = new StringBuffer(2048);

        for (Map.Entry<InetAddress, NodeContext> entry : peers.entrySet())
            sb.append(entry.getValue().hpvService).append("\n");

        logger.info(sb.toString());
    }

    class NodeContext
    {
        final HyParViewService hpvService;
        final ScheduledThreadPoolExecutor scheduler;

        NodeContext(InetAddress addr, String datacenter, SeedProvider seedProvider,
                           SimulationMessageSender messageSender)
        {
            hpvService = new HyParViewService(addr, datacenter, Math.abs(random.nextInt()), seedProvider, 3);
            scheduler = new ScheduledThreadPoolExecutor(1);
            hpvService.testInit(messageSender, scheduler, scheduler);
        }

        void send(HyParViewMessage message, long delay)
        {
            logger.info(String.format("scheduling [%s] to %s", message.toString(), hpvService.getLocalAddress()));
            scheduler.schedule(() -> deliver(message), delay, TimeUnit.MILLISECONDS);
        }

        void deliver(HyParViewMessage message)
        {
            while (paused.get())
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
            logger.info(String.format("delivering [%s] to %s", message.toString(), hpvService.getLocalAddress()));
            hpvService.receiveMessage(message);
        }

        public void awaitQuiesence()
        {
            // note sure if there's a way to force the execution of tasks, 'cuz blocking sure sucks
            // getActiveCount may not be the API to use here...
            while (scheduler.getActiveCount() != 0)
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
        }
    }

    static class SimulationSeedProvider implements SeedProvider
    {
        private final List<InetAddress> seeds;

        SimulationSeedProvider(List<InetAddress> seeds)
        {
            this.seeds = seeds;
        }

        public List<InetAddress> getSeeds()
        {
            return seeds;
        }
    }
}
