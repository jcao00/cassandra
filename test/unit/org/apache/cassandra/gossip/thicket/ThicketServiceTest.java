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
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;
import org.apache.cassandra.gossip.thicket.ThicketService.BroadcastPeers;
import org.apache.cassandra.gossip.thicket.ThicketService.MissingMessges;
import org.apache.cassandra.gossip.thicket.ThicketService.MissingSummary;

public class ThicketServiceTest
{
    static final int SEED = 92342784;
    private static InetAddress localNodeAddr;
    static GossipMessageId.IdGenerator idGenerator = new GossipMessageId.IdGenerator(42);
    ExecutorService executorService;
    ScheduledExecutorService scheduler;
    Random random;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        localNodeAddr = InetAddress.getByName("127.0.0.1");
    }

    @Before
    public void setup()
    {
        executorService = new NopExecutorService();
        scheduler = new NopExecutorService();
        random = new Random(SEED);
    }

    private ThicketService createService(int peersCount)
    {
        ThicketService thicket = new ThicketService(localNodeAddr, new TestMessageSender(), executorService, scheduler);
        thicket.start(new SimplePeerSamplingService(peersCount), 42);
        return thicket;
    }

    @Test
    public void selectBroadcastPeers_Empty()
    {
        BroadcastPeers broadcastPeers = ThicketService.selectBroadcastPeers(localNodeAddr, Collections.emptyList(), Collections.emptyList(), 5);
        Assert.assertTrue(broadcastPeers.active.isEmpty());
        Assert.assertTrue(broadcastPeers.backup.isEmpty());
    }

    @Test
    public void selecBroadcastPeers_SmallPeersList() throws UnknownHostException
    {
        int maxSize = 5;
        ThicketService thicket = createService(maxSize - 2);

        BroadcastPeers broadcastPeers = ThicketService.selectBroadcastPeers(localNodeAddr, Collections.emptyList(), thicket.getBackupPeers(), maxSize);
        Assert.assertFalse(broadcastPeers.active.isEmpty());
        Assert.assertTrue(broadcastPeers.backup.isEmpty());
        Assert.assertFalse(Collections.disjoint(broadcastPeers.active, thicket.getBackupPeers()));
    }

    @Test
    public void selectBroadcastPeers_LargePeersList() throws UnknownHostException
    {
        int maxSize = 5;
        ThicketService thicket = createService(maxSize + 2);

        BroadcastPeers broadcastPeers = ThicketService.selectBroadcastPeers(localNodeAddr, Collections.emptyList(), thicket.getBackupPeers(), maxSize);
        Assert.assertFalse(broadcastPeers.active.isEmpty());
        Assert.assertFalse(broadcastPeers.backup.isEmpty());
        Assert.assertFalse(Collections.disjoint(broadcastPeers.active, thicket.getBackupPeers()));
        Assert.assertFalse(Collections.disjoint(broadcastPeers.backup, thicket.getBackupPeers()));
    }

    @Test
    public void selectBroadcastPeers_LargePeersList_WithExistingPeers() throws UnknownHostException
    {
        int maxSize = 5;
        ThicketService thicket = createService(maxSize + 2);

        List<InetAddress> existingPeers = new LinkedList<>();
        InetAddress peer = thicket.getBackupPeers().iterator().next();
        existingPeers.add(peer);

        BroadcastPeers broadcastPeers = ThicketService.selectBroadcastPeers(localNodeAddr, existingPeers, thicket.getBackupPeers(), maxSize);
        Assert.assertFalse(broadcastPeers.active.isEmpty());
        Assert.assertFalse(broadcastPeers.backup.isEmpty());
        Assert.assertFalse(Collections.disjoint(broadcastPeers.active, thicket.getBackupPeers()));
        Assert.assertFalse(Collections.disjoint(broadcastPeers.backup, thicket.getBackupPeers()));
    }

    @Test
    public void broadcast_NoPeers()
    {
        // might be the first (and only) node in the cluster (think standalone testing)
        ThicketService thicket = createService(0);
        thicket.performBroadcast("testing..1..2..3", new SimpleClient());

        TestMessageSender messageSender = (TestMessageSender) thicket.messageSender;
        Assert.assertTrue(messageSender.messages.isEmpty());
    }

    @Test
    public void broadcast_WithPeers()
    {
        int peersSize = 3;
        ThicketService thicket = createService(peersSize);
        thicket.performBroadcast("testing..1..2..3", new SimpleClient());

        TestMessageSender messageSender = (TestMessageSender) thicket.messageSender;
        Assert.assertFalse(messageSender.messages.isEmpty());
    }

    @Test
    public void removeFromMissing_RemoveEmptyTree() throws UnknownHostException
    {
        List<MissingMessges> missing = new LinkedList<>();
        InetAddress treeRoot = InetAddress.getByName("127.0.1.23");
        GossipMessageId messageId = idGenerator.generate();

        MissingMessges msgs = new MissingMessges(42);
        MissingSummary summary = new MissingSummary();
        summary.add(InetAddress.getByName("127.8.1.55"), Collections.singletonList(messageId));
        msgs.trees.put(treeRoot, summary);
        missing.add(msgs);

        ThicketService.removeFromMissing(missing, treeRoot, messageId);
        Assert.assertTrue(msgs.trees.isEmpty());
    }

    @Test
    public void removeFromMissing_NonEmptyTree() throws UnknownHostException
    {
        List<MissingMessges> missing = new LinkedList<>();
        InetAddress treeRoot = InetAddress.getByName("127.0.1.23");
        GossipMessageId messageId = idGenerator.generate();

        MissingMessges msgs = new MissingMessges(42);
        MissingSummary summary = new MissingSummary();
        List<GossipMessageId> msgIds = new LinkedList<>();
        msgIds.add(messageId);
        msgIds.add(idGenerator.generate());

        summary.add(InetAddress.getByName("127.8.1.55"), msgIds);
        msgs.trees.put(treeRoot, summary);
        missing.add(msgs);

        ThicketService.removeFromMissing(missing, treeRoot, messageId);
        MissingSummary missingSummary = msgs.trees.get(treeRoot);
        Assert.assertNotNull(missingSummary);
        Assert.assertEquals(1, missingSummary.messages.size());
    }

    @Test
    public void relayMessage_IsLeaf_EmptyBackupPeers() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        InetAddress sender = InetAddress.getByName("127.123.234.1");
        DataMessage msg = new DataMessage(sender, idGenerator.generate(), sender, "ThisIsThePayload", "client", Collections.emptyList());
        thicket.relayMessage(msg);
        TestMessageSender messageSender = (TestMessageSender)thicket.messageSender;
        Assert.assertTrue(messageSender.messages.isEmpty());
    }

    @Test
    public void relayMessage_IsLeaf_AlreadyInterior() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        // set up another tree for which our node is interior
        Map<InetAddress, BroadcastPeers> broadcastPeers = thicket.getBroadcastPeers();
        Set<InetAddress> active = new HashSet<InetAddress>()
        {{
            add(InetAddress.getByName("127.123.234.77"));
            add(InetAddress.getByName("127.123.234.78"));
            add(InetAddress.getByName("127.123.234.79"));
        }};

        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, new BroadcastPeers(active, Collections.emptySet()));

        InetAddress sender = InetAddress.getByName("127.123.234.1");
        DataMessage msg = new DataMessage(sender, idGenerator.generate(), sender, "ThisIsThePayload", "client", Collections.emptyList());
        thicket.relayMessage(msg);
        TestMessageSender messageSender = (TestMessageSender)thicket.messageSender;
        Assert.assertTrue(messageSender.messages.isEmpty());
    }

    @Test
    public void relayMessage_IsInterior() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        // set up another tree for which our node is interior
        Map<InetAddress, BroadcastPeers> broadcastPeers = thicket.getBroadcastPeers();
        Set<InetAddress> active = new HashSet<InetAddress>()
        {{
            add(InetAddress.getByName("127.123.234.77"));
            add(InetAddress.getByName("127.123.234.78"));
            add(InetAddress.getByName("127.123.234.79"));
        }};

        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, new BroadcastPeers(active, Collections.emptySet()));

        DataMessage msg = new DataMessage(treeRoot, idGenerator.generate(), treeRoot, "ThisIsThePayload", "client", Collections.emptyList());
        thicket.relayMessage(msg);
        TestMessageSender messageSender = (TestMessageSender)thicket.messageSender;
        Assert.assertFalse(messageSender.messages.isEmpty());
    }

    @Test
    public void isInterior_EmptyBroadcastPeers()
    {
        ThicketService thicket = createService(3);
        Assert.assertFalse(thicket.isInterior(new HashMap<>()));
    }

    @Test
    public void isInterior_OnlyAsLeaf() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        Map<InetAddress, BroadcastPeers> broadcastPeers = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, new BroadcastPeers(Collections.singleton(treeRoot), Collections.emptySet()));
        }

        Assert.assertFalse(thicket.isInterior(broadcastPeers));

    }

    @Test
    public void isInterior() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        Map<InetAddress, BroadcastPeers> broadcastPeers = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            final int lastOctet = i;
            Set<InetAddress> active = new HashSet<InetAddress>()
            {{
                add(treeRoot);
                add(InetAddress.getByName("127.0.2." + lastOctet));
                add(InetAddress.getByName("127.0.2." + (lastOctet + 10)));
            }};
            broadcastPeers.put(treeRoot, new BroadcastPeers(active, Collections.emptySet()));
        }

        Assert.assertTrue(thicket.isInterior(broadcastPeers));
    }

    @Test
    public void localLoadEstimate_Empty()
    {
        Assert.assertTrue(ThicketService.localLoadEstimate(new HashMap<>()).isEmpty());
    }

    @Test
    public void localLoadEstimate_OnlyLeaves() throws UnknownHostException
    {
        Map<InetAddress, BroadcastPeers> broadcastPeers = new HashMap<>();
        for (int i = 0; i < 4; i ++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, new BroadcastPeers(Collections.singleton(treeRoot), Collections.emptySet()));
        }

        Collection<LoadEstimate> estimates = ThicketService.localLoadEstimate(broadcastPeers);
        Assert.assertTrue(estimates.isEmpty());
    }

    @Test
    public void localLoadEstimate_OneInterior() throws UnknownHostException
    {
        Map<InetAddress, BroadcastPeers> broadcastPeers = new HashMap<>();
        for (int i = 0; i < 4; i ++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, new BroadcastPeers(Collections.singleton(treeRoot), Collections.emptySet()));
        }

        InetAddress treeRoot = InetAddress.getByName("127.1.1.0");
        Set<InetAddress> active = new HashSet<InetAddress>()
        {{
            add(treeRoot);
            add(InetAddress.getByName("127.1.1.1"));
            add(InetAddress.getByName("127.1.1.2"));
        }};
        broadcastPeers.put(treeRoot, new BroadcastPeers(active, Collections.emptySet()));

        Collection<LoadEstimate> estimates = ThicketService.localLoadEstimate(broadcastPeers);
        Assert.assertEquals(1, estimates.size());
        LoadEstimate estimate = estimates.iterator().next();
        Assert.assertEquals(treeRoot, estimate.treeRoot);
        Assert.assertEquals(2, estimate.load);
    }


    @Test
    public void filterMissingMessages_Simple() throws UnknownHostException
    {
        ThicketService thicket = createService(1);

        InetAddress treeRoot = InetAddress.getByName("127.97.21.1");
        GossipMessageId messageId = idGenerator.generate();

        Multimap<InetAddress, GossipMessageId> summary = HashMultimap.create();
        summary.put(treeRoot, messageId);

        Multimap<InetAddress, GossipMessageId> seen = HashMultimap.create();
        seen.put(treeRoot, messageId);

        Assert.assertFalse(summary.isEmpty());
        Assert.assertTrue(thicket.filterMissingMessages(summary, seen).isEmpty());
    }

    @Test
    public void filterMissingMessages_MultipleIds() throws UnknownHostException
    {
        ThicketService thicket = createService(1);

        InetAddress treeRoot = InetAddress.getByName("127.97.21.1");
        GossipMessageId messageId = idGenerator.generate();

        Multimap<InetAddress, GossipMessageId> summary = HashMultimap.create();
        summary.put(treeRoot, messageId);
        summary.put(treeRoot, idGenerator.generate());

        Multimap<InetAddress, GossipMessageId> seen = HashMultimap.create();
        seen.put(treeRoot, messageId);

        Assert.assertFalse(summary.isEmpty());
        Multimap<InetAddress, GossipMessageId> filtered = thicket.filterMissingMessages(summary, seen);
        Assert.assertFalse(filtered.isEmpty());
        Collection<GossipMessageId> gossipMessageIds = filtered.get(treeRoot);
        Assert.assertEquals(1, gossipMessageIds.size());
    }

    @Test
    public void addToMissingMessages_EmptyExisting() throws UnknownHostException
    {
        InetAddress treeRoot = InetAddress.getByName("127.97.21.1");
        GossipMessageId messageId = idGenerator.generate();

        Multimap<InetAddress, GossipMessageId> reportedMissing = HashMultimap.create();
        reportedMissing.put(treeRoot, messageId);

        InetAddress sender = InetAddress.getByName("127.87.12.221");
        List<MissingMessges> missing = new LinkedList<>();
        ThicketService.addToMissingMessages(sender, missing, reportedMissing, ThicketService.SUMMARY_RETENTION_TIME);

        Assert.assertFalse(missing.isEmpty());
        MissingMessges missingMessges = missing.get(0);
        MissingSummary missingSummary = missingMessges.trees.get(treeRoot);
        Assert.assertNotNull(missingSummary);
        Assert.assertTrue(missingSummary.messages.contains(messageId));
        Assert.assertTrue(missingSummary.peers.contains(sender));
    }

    @Test
    public void addToMissingMessages_AddToExisting() throws UnknownHostException
    {
        InetAddress treeRoot = InetAddress.getByName("127.97.21.1");
        GossipMessageId messageId = idGenerator.generate();
        GossipMessageId messageId2 = idGenerator.generate();

        Multimap<InetAddress, GossipMessageId> reportedMissing = HashMultimap.create();
        reportedMissing.put(treeRoot, messageId);
        reportedMissing.put(treeRoot, messageId2);

        InetAddress sender = InetAddress.getByName("127.87.12.221");
        List<MissingMessges> existingMissing = new LinkedList<>();
        MissingMessges mm = new MissingMessges(42);
        MissingSummary sm = new MissingSummary();
        sm.add(sender, Collections.singletonList(messageId));
        mm.trees.put(treeRoot, sm);

        ThicketService.addToMissingMessages(sender, existingMissing, reportedMissing, ThicketService.SUMMARY_RETENTION_TIME);

        Assert.assertFalse(existingMissing.isEmpty());
        MissingMessges missingMessges = existingMissing.get(0);
        MissingSummary missingSummary = missingMessges.trees.get(treeRoot);
        Assert.assertNotNull(missingSummary);
        Assert.assertTrue(missingSummary.messages.contains(messageId));
        Assert.assertTrue(missingSummary.messages.contains(messageId2));
        Assert.assertTrue(missingSummary.peers.contains(sender));
    }

    @Test
    public void discoverGraftCandidates_NoneReadyForEvaluation()
    {
        List<MissingMessges> missingMessges = new LinkedList<>();
        MissingMessges msgs = new MissingMessges(System.nanoTime());
        missingMessges.add(msgs);
        Assert.assertNull(ThicketService.discoverGraftCandidates(missingMessges));
    }

    @Test
    public void discoverGraftCandidates_OnlyOnePastTimestamp() throws UnknownHostException
    {
        List<MissingMessges> missingMessges = new LinkedList<>();
        MissingMessges msgs = new MissingMessges(System.nanoTime() - TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS));
        missingMessges.add(msgs);
        MissingSummary summary = new MissingSummary();

        InetAddress summarizer1 = InetAddress.getByName("127.1.1.1");
        summary.add(summarizer1, Collections.singleton(idGenerator.generate()));
        InetAddress summarizer2 = InetAddress.getByName("127.1.1.2");
        summary.add(summarizer2, Collections.singleton(idGenerator.generate()));
        InetAddress treeRoot1 = InetAddress.getByName("127.0.1.1");
        msgs.trees.put(treeRoot1, summary);

        msgs = new MissingMessges(System.nanoTime() + TimeUnit.NANOSECONDS.convert(20, TimeUnit.SECONDS));
        missingMessges.add(msgs);
        summary = new MissingSummary();
        InetAddress summarizer3 = InetAddress.getByName("127.1.1.3");
        summary.add(summarizer3, Collections.singleton(idGenerator.generate()));
        InetAddress treeRoot2 = InetAddress.getByName("127.0.1.2");
        msgs.trees.put(treeRoot2, summary);

        Multimap<InetAddress, InetAddress> graftCandidates = ThicketService.discoverGraftCandidates(missingMessges);
        Assert.assertNotNull(graftCandidates);
        Assert.assertEquals(1, graftCandidates.keySet().size());
        Collection<InetAddress> peers = graftCandidates.get(treeRoot1);
        Assert.assertEquals(2, peers.size());
        Assert.assertTrue(peers.contains(summarizer1));
        Assert.assertTrue(peers.contains(summarizer2));
    }

    @Test
    public void detetmineBestCandidate_SimpleDeny() throws UnknownHostException
    {
        Multimap<InetAddress, LoadEstimate> estimates = HashMultimap.create();
        List<InetAddress> candidates = new LinkedList<>();
        InetAddress peer = InetAddress.getByName("127.0.1.1");
        candidates.add(peer);
        estimates.put(peer, new LoadEstimate(InetAddress.getByName("127.1.1.0"), 4));
        estimates.put(peer, new LoadEstimate(InetAddress.getByName("127.1.1.1"), 1));

        Optional<InetAddress> target = ThicketService.detetmineBestCandidate(estimates, candidates, 5);
        Assert.assertFalse(target.isPresent());
    }

    @Test
    public void detetmineBestCandidate_SimpleOK() throws UnknownHostException
    {
        Multimap<InetAddress, LoadEstimate> estimates = HashMultimap.create();
        List<InetAddress> candidates = new LinkedList<>();
        InetAddress peer = InetAddress.getByName("127.0.1.1");
        candidates.add(peer);
        estimates.put(peer, new LoadEstimate(InetAddress.getByName("127.1.1.0"), 3));

        Optional<InetAddress> target = ThicketService.detetmineBestCandidate(estimates, candidates, 5);
        Assert.assertTrue(target.isPresent());
        Assert.assertEquals(peer, target.get());
    }

    @Test
    public void isOverMaxLoad_EmptyEstimates()
    {
        Assert.assertFalse(ThicketService.isOverMaxLoad(Collections.emptyList(), 2));
    }

    @Test
    public void isOverMaxLoad_InManyTrees() throws UnknownHostException
    {
        int maxTrees = 3;
        List<LoadEstimate> estimates = new LinkedList<>();
        for (int i = 0; i < maxTrees; i ++)
            estimates.add(new LoadEstimate(InetAddress.getByName("127.0.0." + i), 1));

        Assert.assertTrue(ThicketService.isOverMaxLoad(estimates, 7));
    }

    @Test
    public void isOverMaxLoad_AtMaxLoad() throws UnknownHostException
    {
        int maxLoad = 5;
        List<LoadEstimate> estimates = new LinkedList<>();
        estimates.add(new LoadEstimate(InetAddress.getByName("127.0.0.2"), 1));
        estimates.add(new LoadEstimate(InetAddress.getByName("127.0.0.3"), maxLoad - 1));

        Assert.assertTrue(ThicketService.isOverMaxLoad(estimates, maxLoad));
    }

    @Test
    public void handleGraft_UnderMaxLoad() throws UnknownHostException
    {
        ThicketService thicket = createService(5);
        InetAddress sender = thicket.getBackupPeers().iterator().next();
        InetAddress treeRoot1 = InetAddress.getByName("127.1.1.1");
        InetAddress treeRoot2 = InetAddress.getByName("127.1.1.2");
        List<InetAddress> roots = new LinkedList<InetAddress>() {{ add(treeRoot1); add(treeRoot2); }};

        Map<InetAddress, BroadcastPeers> broadcastPeers = thicket.getBroadcastPeers();
        broadcastPeers.put(treeRoot1, new BroadcastPeers(new HashSet<InetAddress>() {{ add(treeRoot1); }}, new HashSet<>()));
        broadcastPeers.put(treeRoot2, new BroadcastPeers(new HashSet<InetAddress>() {{ add(treeRoot2); }}, new HashSet<>()));

        thicket.handleGraft(new GraftMessage(sender, idGenerator.generate(), roots, Collections.emptyList()));
        broadcastPeers = thicket.getBroadcastPeers();

        BroadcastPeers bcastPeers = broadcastPeers.get(treeRoot1);
        Assert.assertNotNull(bcastPeers);
        Assert.assertTrue(bcastPeers.active.contains(sender));

        bcastPeers = broadcastPeers.get(treeRoot2);
        Assert.assertNotNull(bcastPeers);
        Assert.assertTrue(bcastPeers.active.contains(sender));

        Assert.assertTrue(((TestMessageSender) thicket.messageSender).messages.isEmpty());
    }

    @Test
    public void handleGraft_OverMaxLoad() throws UnknownHostException
    {
        ThicketService thicket = createService(1);
        InetAddress sender = thicket.getBackupPeers().iterator().next();
        Map<InetAddress, BroadcastPeers> broadcastPeers = thicket.getBroadcastPeers();
        Set<InetAddress> active = new HashSet<InetAddress>()
        {{
            add(InetAddress.getByName("127.123.234.77"));
            add(InetAddress.getByName("127.123.234.78"));
            add(InetAddress.getByName("127.123.234.79"));
        }};

        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, new BroadcastPeers(active, Collections.emptySet()));

        InetAddress treeRoot1 = InetAddress.getByName("127.1.1.1");
        InetAddress treeRoot2 = InetAddress.getByName("127.1.1.2");
        List<InetAddress> roots = new LinkedList<InetAddress>() {{ add(treeRoot1); add(treeRoot2); }};
        broadcastPeers.put(treeRoot1, new BroadcastPeers(new HashSet<InetAddress>() {{ add(treeRoot1); }}, new HashSet<>()));
        broadcastPeers.put(treeRoot2, new BroadcastPeers(new HashSet<InetAddress>() {{ add(treeRoot2); }}, new HashSet<>()));

        thicket.handleGraft(new GraftMessage(sender, idGenerator.generate(), roots, Collections.emptyList()));

        broadcastPeers = thicket.getBroadcastPeers();

        BroadcastPeers bcastPeers = broadcastPeers.get(treeRoot1);
        Assert.assertNotNull(bcastPeers);
        Assert.assertFalse(bcastPeers.active.contains(sender));

        bcastPeers = broadcastPeers.get(treeRoot2);
        Assert.assertNotNull(bcastPeers);
        Assert.assertFalse(bcastPeers.active.contains(sender));

        Assert.assertFalse(((TestMessageSender) thicket.messageSender).messages.isEmpty());
    }

    @Test
    public void handlePrune() throws UnknownHostException
    {
        ThicketService thicket = createService(1);
        Map<InetAddress, BroadcastPeers> broadcastPeers = thicket.getBroadcastPeers();
        InetAddress sender = InetAddress.getByName("127.123.234.77");

        Set<InetAddress> active = new HashSet<InetAddress>()
        {{
            add(InetAddress.getByName("127.123.234.77"));
            add(InetAddress.getByName("127.123.234.78"));
            add(InetAddress.getByName("127.123.234.79"));
        }};

        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, new BroadcastPeers(active, new HashSet<InetAddress>()));

        thicket.handlePrune(new PruneMessage(sender, idGenerator.generate(), new LinkedList<InetAddress>() {{ add(treeRoot); }}, Collections.emptyList()));
        broadcastPeers = thicket.getBroadcastPeers();
        BroadcastPeers bcastPeers = broadcastPeers.get(treeRoot);
        Assert.assertNotNull(bcastPeers);
        Assert.assertFalse(bcastPeers.active.contains(sender));
        Assert.assertTrue(thicket.getBackupPeers().contains(sender));
    }

    /*
        testing fixtures
     */

    static class SimplePeerSamplingService implements PeerSamplingService
    {
        final List<InetAddress> peers;

        SimplePeerSamplingService(int backupPeersCount)
        {
            peers = new LinkedList<>();
            try
            {
                for (int i = 0; i < backupPeersCount; i++)
                    peers.add(InetAddress.getByName("127.0.1." + i));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public void start(int epoch)
        {

        }

        public Collection<InetAddress> getPeers()
        {
            return peers;
        }

        public void register(PeerSamplingServiceListener listener)
        {

        }

        public void unregister(PeerSamplingServiceListener listener)
        {

        }

        public void shutdown()
        {

        }
    }

    static class TestMessageSender implements MessageSender<ThicketMessage>
    {
        List<SentMessage> messages = new LinkedList<>();

        public void send(InetAddress destinationAddr, ThicketMessage message)
        {
            messages.add(new SentMessage(destinationAddr, message));
        }
    }

    static class SentMessage
    {
        final InetAddress destination;
        final ThicketMessage message;

        SentMessage(InetAddress destination, ThicketMessage message)
        {
            this.destination = destination;
            this.message = message;
        }
    }

    static class NopExecutorService implements ScheduledExecutorService
    {
        public void execute(Runnable command)
        {

        }

        public void shutdown()
        {

        }

        public List<Runnable> shutdownNow()
        {
            return null;
        }

        public boolean isShutdown()
        {
            return false;
        }

        public boolean isTerminated()
        {
            return false;
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return false;
        }

        public <T> Future<T> submit(Callable<T> task)
        {
            return null;
        }

        public <T> Future<T> submit(Runnable task, T result)
        {
            return null;
        }

        public Future<?> submit(Runnable task)
        {
            return null;
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
        {
            return null;
        }

        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
        {
            return null;
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
        {
            return null;
        }

        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return null;
        }

        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            return null;
        }

        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            return null;
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            return null;
        }
    }
}
