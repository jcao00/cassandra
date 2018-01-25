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

package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class GossiperTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }
    static final IPartitioner partitioner = new RandomPartitioner();
    StorageService ss = StorageService.instance;
    TokenMetadata tmd = StorageService.instance.getTokenMetadata();
    ArrayList<Token> endpointTokens = new ArrayList<>();
    ArrayList<Token> keyTokens = new ArrayList<>();
    List<InetAddress> hosts = new ArrayList<>();
    List<UUID> hostIds = new ArrayList<>();

    private SeedProvider originalSeedProvider;

    @Before
    public void setup()
    {
        tmd.clearUnsafe();
        originalSeedProvider = DatabaseDescriptor.getSeedProvider();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setSeedProvider(originalSeedProvider);
    }

    @Test
    public void testLargeGenerationJump() throws UnknownHostException, InterruptedException
    {
        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, hostIds, 2);
        InetAddress remoteHostAddress = hosts.get(1);

        EndpointState initialRemoteState = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress);
        HeartBeatState initialRemoteHeartBeat = initialRemoteState.getHeartBeatState();

        //Util.createInitialRing should have initialized remoteHost's HeartBeatState's generation to 1
        assertEquals(initialRemoteHeartBeat.getGeneration(), 1);

        HeartBeatState proposedRemoteHeartBeat = new HeartBeatState(initialRemoteHeartBeat.getGeneration() + Gossiper.MAX_GENERATION_DIFFERENCE + 1);
        EndpointState proposedRemoteState = new EndpointState(proposedRemoteHeartBeat);

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, proposedRemoteState));

        //The generation should have been updated because it isn't over Gossiper.MAX_GENERATION_DIFFERENCE in the future
        HeartBeatState actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();
        assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());

        //Propose a generation 10 years in the future - this should be rejected.
        HeartBeatState badProposedRemoteHeartBeat = new HeartBeatState((int) (System.currentTimeMillis()/1000) + Gossiper.MAX_GENERATION_DIFFERENCE * 10);
        EndpointState badProposedRemoteState = new EndpointState(badProposedRemoteHeartBeat);

        Gossiper.instance.applyStateLocally(ImmutableMap.of(remoteHostAddress, badProposedRemoteState));

        actualRemoteHeartBeat = Gossiper.instance.getEndpointStateForEndpoint(remoteHostAddress).getHeartBeatState();

        //The generation should not have been updated because it is over Gossiper.MAX_GENERATION_DIFFERENCE in the future
        assertEquals(proposedRemoteHeartBeat.getGeneration(), actualRemoteHeartBeat.getGeneration());
    }

    @Test
    public void testReloadSeeds() throws UnknownHostException
    {
        Gossiper gossiper = new Gossiper(false);

        InetAddress addr = InetAddress.getByName("127.0.1.1");
        for (int i = 0; i < 4; i++)
        {
            gossiper.seeds.add(addr);
            addr = InetAddresses.increment(addr);
        }
        Assert.assertEquals(4, gossiper.seeds.size());

        addr = InetAddress.getByName("127.0.1.1");
        int nextSize = gossiper.seeds.size() + 1;
        List<InetAddress> nextSeeds = new ArrayList<>(nextSize);
        for (int i = 0; i < nextSize; i ++)
        {
            nextSeeds.add(addr);
            addr = InetAddresses.increment(addr);
        }

        DatabaseDescriptor.setSeedProvider(new TestSeedProvider(nextSeeds));
        gossiper.reloadSeeds();

        Assert.assertEquals(nextSize, gossiper.seeds.size());
        for (InetAddress a : nextSeeds)
            Assert.assertTrue(gossiper.seeds.contains(a));
    }

    static class TestSeedProvider implements SeedProvider
    {
        private List<InetAddress> seeds;

        TestSeedProvider(List<InetAddress> seeds)
        {
            this.seeds = seeds;
        }

        @Override
        public List<InetAddress> getSeeds()
        {
            return seeds;
        }
    }
}
