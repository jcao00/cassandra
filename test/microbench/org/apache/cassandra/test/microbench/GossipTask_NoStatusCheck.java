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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.net.InetAddresses;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Thread)
@Warmup(iterations = 4, time = 4)
@Measurement(iterations = 4, time = 10)
@Fork(4)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.SampleTime)
public class GossipTask_NoStatusCheck
{
    private final Random random = new Random(238746234L);
    private final VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
    private final GossipDigestSynVerbHandler synVerbHandler = new GossipDigestSynVerbHandler();

    private Gossiper.GossipTask gossipTask;
    private MessageIn<GossipDigestSyn> synMessage;

    // TODO:: FBUtil.bcast addr

    @Param({ "16", "128", "512", "1024", "4096", "8192", "12000"})
    private int clusterSize;

    @Setup
    public void setup() throws IOException
    {
        System.setProperty("cassandra.config", "file:////opt/dev/cassandra/conf/cassandra.yaml");
        System.setProperty("cassandra.storagedir", "/tmp/cass");
        DatabaseDescriptor.daemonInitialization();
        Gossiper.instance.executeDoStatusCheck = false;

        MessagingService.instance().setListening();
        MessagingService.instance().clearMessageSinks();
        MessagingService.instance().addMessageSink(new MessageDropper());

        gossipTask = Gossiper.instance.new GossipTask();
        ConcurrentMap<InetAddress, EndpointState> endpointStateMap = new ConcurrentHashMap<>(clusterSize);
        InetAddress addr = FBUtilities.getBroadcastAddress();
        for (int i = 0; i < clusterSize; i++)
        {
            endpointStateMap.put(addr, generateEndpointState(addr));
            // increment for next loop iteration
            addr = InetAddresses.increment(addr);
        }

        Set<InetAddress> endpoints = endpointStateMap.keySet();
        int seedCount = 4;
        Set<InetAddress> seeds = endpoints.stream().limit(seedCount).collect(Collectors.toSet());
        Gossiper.instance.injectEndpointStateMap(endpointStateMap, seeds, endpoints);


        synMessage = buildSyn(endpointStateMap);
    }

    private EndpointState generateEndpointState(InetAddress addr)
    {
        HeartBeatState hbs = new HeartBeatState(random.nextInt(), random.nextInt());
        Map<ApplicationState, VersionedValue> states = new HashMap<>();
        states.put(ApplicationState.RACK, valueFactory.rack("rack1"));
        states.put(ApplicationState.DC, valueFactory.datacenter("dc1"));
        states.put(ApplicationState.HOST_ID, valueFactory.hostId(UUID.randomUUID()));
        states.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());
        states.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
        states.put(ApplicationState.RPC_READY, valueFactory.rpcReady(true));
        states.put(ApplicationState.RPC_ADDRESS, valueFactory.rpcaddress(addr));
        states.put(ApplicationState.LOAD, valueFactory.load(0.55));
        states.put(ApplicationState.SCHEMA, valueFactory.schema(UUID.randomUUID()));

        return new EndpointState(hbs, states);
    }

    private MessageIn<GossipDigestSyn> buildSyn(ConcurrentMap<InetAddress, EndpointState> endpointStateMap)
    {
        List<GossipDigest> digests = new ArrayList<>(clusterSize);

        for (Map.Entry<InetAddress, EndpointState> e : endpointStateMap.entrySet())
            digests.add(new GossipDigest(e.getKey(), e.getValue().getHeartBeatState().getGeneration(), e.getValue().getHeartBeatState().getHeartBeatVersion() + 1));

        GossipDigestSyn syn = new GossipDigestSyn(DatabaseDescriptor.getClusterName(), DatabaseDescriptor.getPartitionerName(), digests);
        return MessageIn.create(InetAddresses.increment(FBUtilities.getBroadcastAddress()),
                                syn,
                                Collections.emptyMap(),
                                MessagingService.Verb.GOSSIP_DIGEST_SYN,
                                (int)System.currentTimeMillis());
    }

    @Benchmark
    public void gossipTask_run()
    {
        gossipTask.run();
    }

    @Benchmark
    public void doSynVerbHandler()
    {
        synVerbHandler.doVerb(synMessage, 42);
    }

    public static void main(String[] args) throws IOException
    {
        GossipTask_NoStatusCheck c = new GossipTask_NoStatusCheck();
        c.setup();
        c.doSynVerbHandler();
    }

    private static class MessageDropper implements IMessageSink
    {
        @Override
        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
        {
            return false;
        }

        @Override
        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return false;
        }
    }
}
