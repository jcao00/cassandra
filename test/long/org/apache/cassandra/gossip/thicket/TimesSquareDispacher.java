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
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipStateChangeListener;
import org.apache.cassandra.gossip.GossipStateChangeListener.GossipProvider;
import org.apache.cassandra.gossip.hyparview.PennStationDispatcher;
import org.apache.cassandra.gossip.hyparview.SimulationMessageSender;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.utils.Pair;

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
        SimulationMessageSender messageSender = new SimulationMessageSender(this);
        peers.put(addr, new BroadcastNodeContext(addr, datacenter, seedProvider, messageSender));
    }

    public void sendMessage(InetAddress destination, ThicketMessage message)
    {
        if (!containsNode(destination))
            return;

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

    protected class BroadcastNodeContext extends NodeContext
    {
        final ThicketService thicketService;
        final GossipStateChangeListener client;
        private final VersionedValueFactory valueFactory;


        BroadcastNodeContext(InetAddress addr, String datacenter, SeedProvider seedProvider, SimulationMessageSender messageSender)
        {
            super(addr, datacenter, seedProvider, messageSender);
            valueFactory = new VersionedValueFactory(Murmur3Partitioner.instance);
            BroadcastMessageSender broadcastMessageSender = new BroadcastMessageSender(TimesSquareDispacher.this);
            // reuse the schedule from the parent class, assuming hyparview and thicket execute in the thread (for each node)
            long summaryRetentionNanos = TimeUnit.NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
            thicketService = new ThicketService(addr, broadcastMessageSender, scheduler, scheduler, summaryRetentionNanos);
            hpvService.register(thicketService);
            int epoch = Math.abs(random.nextInt());
            thicketService.start(hpvService, epoch, 0, 5, 10, 100);
            client = new GossipStateChangeListener(addr, thicketService, new FakeGossiper(hpvService.endpointStateSubscriber, epoch, valueFactory));
            thicketService.register(client);
        }

        public void broadcastUpdate()
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

        public void shutdown()
        {
            super.shutdown();
            thicketService.shutdown();
        }
    }

    /**
     * A rudimentary version of {@link Gossiper}, but pluggable for testing
     */
    public static class FakeGossiper implements GossipProvider
    {
        private final VersionedValueFactory valueFactory;
        private final IEndpointStateChangeSubscriber stateChangeSubscriber;
        private final EndpointState endpointState;
        private final Set<InetAddress> livePeers;

        public FakeGossiper(IEndpointStateChangeSubscriber stateChangeSubscriber, int epoch, VersionedValueFactory valueFactory)
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

        public EndpointState getLocalState()
        {
            endpointState.getHeartBeatState().updateHeartBeat();
            return endpointState;
        }

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
