package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.AbstractIterator;
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

    private final Random random;
    private final AtomicInteger currentlyProcessingCount = new AtomicInteger();
    private final AtomicInteger totalMessagesSent = new AtomicInteger();
    private final boolean verbose;

    public PennStationDispatcher(boolean verbose)
    {
        this.verbose = verbose;
        this.random = new Random(SEED);
        peers = new ConcurrentHashMap<>();
        seedProvider = new SimulationSeedProvider();
    }

    public void addPeer(InetAddress addr, String datacenter)
    {
        SimulationMessageSender messageSender = new SimulationMessageSender(addr, this);
        peers.put(addr, new NodeContext(addr, datacenter, seedProvider, messageSender));
    }

    public void addSeed(InetAddress seeds)
    {
        seedProvider.getSeeds().add(seeds);
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
            if (seedProvider.getSeeds().contains(destination))
                return;
            logger.info(String.format("%s sending [%s] to non-existent node %s - might be ok", source, message, destination));
            return;
        }

        totalMessagesSent.incrementAndGet();
        peers.get(destination).send(message, 0);
    }

    public void checkActiveView(InetAddress destination)
    {
        if (!peers.containsKey(destination))
        {
            if (seedProvider.getSeeds().contains(destination))
                return;
            logger.info(String.format("sending runnable to non-existent node %s - might be ok", destination));
            return;
        }
        peers.get(destination).sendCheckActiveView(0);
    }

    public void awaitQuiesence()
    {
        while (currentlyProcessingCount.get() > 0)
            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);
    }

    public int totalMessagesSent()
    {
        return totalMessagesSent.get();
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
        sb.append('\n');
        for (Map.Entry<InetAddress, NodeContext> entry : peers.entrySet())
            sb.append(entry.getValue().hpvService).append('\n');

        logger.info(sb.toString());
    }

    public Iterable<HyParViewService> getNodes()
    {
        final Iterator<NodeContext> knownPeers = peers.values().iterator();
        return () -> new AbstractIterator<HyParViewService>()
        {
            protected HyParViewService computeNext()
            {
                while (knownPeers.hasNext())
                    return knownPeers.next().hpvService;
                return endOfData();
            }
        };
    }

    public Set<InetAddress> getPeers()
    {
        return peers.keySet();
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
//            if (verbose)
//                logger.info(String.format("scheduling [%s] to %s", message.toString(), hpvService.getLocalAddress()));
            currentlyProcessingCount.incrementAndGet();
            scheduler.schedule(() -> deliver(message), delay, TimeUnit.MILLISECONDS);
        }

        void deliver(HyParViewMessage message)
        {
            if (verbose)
                logger.info(String.format("delivering [%s] to %s", message.toString(), hpvService.getLocalAddress()));
            hpvService.receiveMessage(message);
            currentlyProcessingCount.decrementAndGet();
        }

        void sendCheckActiveView(long delay)
        {
            currentlyProcessingCount.incrementAndGet();
            scheduler.schedule(() -> deliverCheckActiveView(), delay, TimeUnit.MILLISECONDS);

        }

        void deliverCheckActiveView()
        {
            hpvService.checkFullActiveView();
            currentlyProcessingCount.decrementAndGet();
        }
    }

    static class SimulationSeedProvider implements SeedProvider
    {
        private final List<InetAddress> seeds;

        SimulationSeedProvider()
        {
            this.seeds = new ArrayList<>();
        }

        public List<InetAddress> getSeeds()
        {
            return seeds;
        }
    }
}
