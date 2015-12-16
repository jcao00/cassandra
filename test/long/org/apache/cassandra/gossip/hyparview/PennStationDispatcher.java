/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    protected final Map<InetAddress, NodeContext> peers;
    protected final SeedProvider seedProvider;

    protected final Random random;
    protected final AtomicInteger currentlyProcessingCount = new AtomicInteger();
    protected final AtomicInteger totalMessagesSent = new AtomicInteger();
    protected final boolean verbose;

    public PennStationDispatcher(boolean verbose)
    {
        this.verbose = verbose;
        this.random = new Random(SEED);
        peers = new ConcurrentHashMap<>();
        seedProvider = new SimulationSeedProvider();
    }

    public void addPeer(InetAddress addr, String datacenter)
    {
        SimulationMessageSender messageSender = new SimulationMessageSender(this);
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

    public void sendMessage(InetAddress destination, HyParViewMessage message)
    {
        if (!containsNode(destination))
            return;
        totalMessagesSent.incrementAndGet();
        peers.get(destination).send(message, 0);
    }

    protected boolean containsNode(InetAddress addr)
    {
        if (!peers.containsKey(addr))
        {
            if (seedProvider.getSeeds().contains(addr))
                return false;
            logger.info(String.format("sending not found %s - might be ok", addr));
            return false;
        }
        return true;
    }

    public void checkActiveView(InetAddress destination)
    {
        if (containsNode(destination))
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
            context.shutdown();
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

    public class NodeContext
    {
        protected final HyParViewService hpvService;
        protected final ScheduledThreadPoolExecutor scheduler;

        protected NodeContext(InetAddress addr, String datacenter, SeedProvider seedProvider,
                              SimulationMessageSender messageSender)
        {
            scheduler = new ScheduledThreadPoolExecutor(1);
            hpvService = new HyParViewService(addr, datacenter, seedProvider, messageSender, scheduler, scheduler, 3);
            hpvService.testInit(Math.abs(random.nextInt()), 10, 100);
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

        public void shutdown()
        {
            scheduler.shutdown();
            // do not call HyParViewService.shutdown() as it will send out more messages to peers,
            // announcing the shutdown - which we don't care about about here
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
