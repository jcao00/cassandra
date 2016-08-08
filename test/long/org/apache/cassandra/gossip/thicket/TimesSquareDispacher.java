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
package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue.VersionedValueFactory;
import org.apache.cassandra.gossip.GossipStateChangeListener;
import org.apache.cassandra.gossip.GossipStateChangeListener.GossipProvider;
import org.apache.cassandra.gossip.hyparview.HyParViewMessage;
import org.apache.cassandra.gossip.hyparview.PennStationDispatcher;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.net.MessageOut;

class TimesSquareDispacher extends PennStationDispatcher
{
    private static final Logger logger = LoggerFactory.getLogger(TimesSquareDispacher.class);
    private List<NodeContext> contexts;

    TimesSquareDispacher(boolean verbose)
    {
        super(verbose);
    }

    @Override
    public void addPeer(InetAddress addr, String datacenter)
    {
        peers.put(addr, new BroadcastNodeContext(addr, datacenter, seedProvider));
    }

    @Override
    public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
    {
        if (message.payload instanceof HyParViewMessage)
            return super.allowOutgoingMessage(message, id, to);

        if (containsNode(to))
        {
            totalMessagesSent.incrementAndGet();
            ((BroadcastNodeContext)peers.get(to)).send((ThicketMessage)message.payload, 0);
        }
        return false;
    }

    BroadcastNodeContext selectRandom()
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

    class BroadcastNodeContext extends NodeContext
    {
        final ThicketService thicketService;
        final GossipStateChangeListener client;
        private final VersionedValueFactory valueFactory;

        BroadcastNodeContext(InetAddress addr, String datacenter, SeedProvider seedProvider)
        {
            super(addr, datacenter, seedProvider);
            valueFactory = new VersionedValueFactory(Murmur3Partitioner.instance);
            // reuse the schedule from the parent class, assuming hyparview and thicket execute in the thread (for each node)
            long summaryRetentionNanos = TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
            thicketService = new ThicketService(addr, scheduler, scheduler, summaryRetentionNanos);
            hpvService.register(thicketService);
            int epoch = Math.abs(random.nextInt());
            thicketService.start(hpvService, epoch, 0, 5, 10, 100);
            client = new GossipStateChangeListener(addr, thicketService, new FakeGossiper(hpvService.endpointStateSubscriber, epoch, valueFactory));
            thicketService.register(client);
        }

        void broadcastUpdate()
        {
            client.onChange(thicketService.getLocalAddress(), ApplicationState.DC, valueFactory.datacenter("some_datacenter"));
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

        @Override
        public void shutdown()
        {
            super.shutdown();
            thicketService.shutdown();
        }
    }

    /**
     * A rudimentary version of {@link Gossiper}, but pluggable for testing
     */
    private static class FakeGossiper implements GossipProvider
    {
        private final VersionedValueFactory valueFactory;
        private final IEndpointStateChangeSubscriber stateChangeSubscriber;
        private final EndpointState endpointState;
        private final Set<InetAddress> livePeers;

        FakeGossiper(IEndpointStateChangeSubscriber stateChangeSubscriber, int epoch, VersionedValueFactory valueFactory)
        {
            this.stateChangeSubscriber = stateChangeSubscriber;
            this.valueFactory = valueFactory;
            livePeers = new HashSet<>();

            endpointState = new EndpointState(new HeartBeatState(epoch));
            // add in some values to make it look like a real endpoint
            endpointState.addApplicationState(ApplicationState.DC, valueFactory.datacenter("some_datacenter"));
            endpointState.addApplicationState(ApplicationState.RACK, valueFactory.rack("some_rack"));
            endpointState.addApplicationState(ApplicationState.LOAD, valueFactory.load(310.5));
        }

        @Override
        public EndpointState getLocalState()
        {
            endpointState.getHeartBeatState().updateHeartBeat();
            return endpointState;
        }

        @Override
        public boolean receive(InetAddress addr, EndpointState state)
        {
            if (livePeers.add(addr))
            {
                // pass some dummy versionedvalue, just to trigger the
                stateChangeSubscriber.onChange(addr, ApplicationState.DC, valueFactory.datacenter("some_datacenter"));
            }

            return true;
        }
    }
}
