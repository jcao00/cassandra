package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
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
    public void selectRootBroadcastPeers_Empty()
    {
        ThicketService thicket = createService(0);
        Assert.assertTrue(thicket.selectRootBroadcastPeers(Collections.emptyList(), 5).isEmpty());
    }

    @Test
    public void selectRootBroadcastPeers_SmallPeersList() throws UnknownHostException
    {
        int maxSize = 5;
        ThicketService thicket = createService(maxSize - 2);

        Collection<InetAddress> broadcastPeers = thicket.selectRootBroadcastPeers(thicket.getBackupPeers(), maxSize);
        Assert.assertFalse(broadcastPeers.isEmpty());
        Assert.assertFalse(Collections.disjoint(broadcastPeers, thicket.getBackupPeers()));
    }

    @Test
    public void selectRootBroadcastPeers_LargePeersList() throws UnknownHostException
    {
        int maxSize = 5;
        ThicketService thicket = createService(maxSize + 2);

        Collection<InetAddress> broadcastPeers = thicket.selectRootBroadcastPeers(thicket.getBackupPeers(), maxSize);
        Assert.assertFalse(broadcastPeers.isEmpty());
        Assert.assertFalse(Collections.disjoint(broadcastPeers, thicket.getBackupPeers()));
    }

    @Test
    public void broadcast_NoPeers()
    {
        // might be the first (and only) node in the cluster (think standalone testing)
        ThicketService thicket = createService(0);
        thicket.broadcast("testing..1..2..3", new SimpleClient());

        TestMessageSender messageSender = (TestMessageSender) thicket.messageSender;
        Assert.assertTrue(messageSender.messages.isEmpty());
    }

    @Test
    public void broadcast_WithPeers()
    {
        int peersSize = 3;
        ThicketService thicket = createService(peersSize);
        thicket.broadcast("testing..1..2..3", new SimpleClient());

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

        ThicketService thicket = createService(1);
        thicket.removeFromMissing(missing, treeRoot, messageId);
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

        ThicketService thicket = createService(1);
        thicket.removeFromMissing(missing, treeRoot, messageId);
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
        Multimap<InetAddress, InetAddress> broadcastPeers = thicket.getBroadcastPeers();
        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.77"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.78"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.79"));

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
        Multimap<InetAddress, InetAddress> broadcastPeers = thicket.getBroadcastPeers();
        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.77"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.78"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.79"));

        DataMessage msg = new DataMessage(treeRoot, idGenerator.generate(), treeRoot, "ThisIsThePayload", "client", Collections.emptyList());
        thicket.relayMessage(msg);
        TestMessageSender messageSender = (TestMessageSender)thicket.messageSender;
        Assert.assertFalse(messageSender.messages.isEmpty());
    }

    @Test
    public void isInterior_EmptyBroadcastPeers()
    {
        ThicketService thicket = createService(3);
        Assert.assertFalse(thicket.isInterior(HashMultimap.create()));
    }

    @Test
    public void isInterior_OnlyAsLeaf() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        HashMultimap<InetAddress, InetAddress> broadcastPeers = HashMultimap.create();
        for (int i = 0; i < 4; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, treeRoot);
        }

        Assert.assertFalse(thicket.isInterior(broadcastPeers));

    }

    @Test
    public void isInterior() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        HashMultimap<InetAddress, InetAddress> broadcastPeers = HashMultimap.create();
        for (int i = 0; i < 4; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, treeRoot);
            broadcastPeers.put(treeRoot, InetAddress.getByName("127.0.2." + i));
            broadcastPeers.put(treeRoot, InetAddress.getByName("127.0.2." + (i + 10)));
        }

        Assert.assertTrue(thicket.isInterior(broadcastPeers));

    }

    @Test
    public void selectBranchBroadcastPeers_NoBackupPeers() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        InetAddress upstream = InetAddress.getByName("127.0.0.233");
        Collection<InetAddress> broadcastPeers = thicket.selectBranchBroadcastPeers(upstream, 2);
        Assert.assertEquals(1, broadcastPeers.size());
        Assert.assertTrue(broadcastPeers.contains(upstream));
        Assert.assertFalse(thicket.getBackupPeers().contains(upstream));
        Assert.assertTrue(Collections.disjoint(broadcastPeers, thicket.getBackupPeers()));
    }

    @Test
    public void selectBranchBroadcastPeers_InBackupPeers() throws UnknownHostException
    {
        ThicketService thicket = createService(3);
        InetAddress upstream = thicket.getBackupPeers().iterator().next();
        Collection<InetAddress> broadcastPeers = thicket.selectBranchBroadcastPeers(upstream, 2);
        Assert.assertEquals(2, broadcastPeers.size());
        Assert.assertTrue(broadcastPeers.contains(upstream));
        Assert.assertFalse(thicket.getBackupPeers().contains(upstream));
        Assert.assertTrue(Collections.disjoint(broadcastPeers, thicket.getBackupPeers()));
    }

    @Test
    public void localLoadEstimate_Empty()
    {
        Assert.assertTrue(ThicketService.localLoadEstimate(HashMultimap.create()).isEmpty());
    }

    @Test
    public void localLoadEstimate_OnlyLeaves() throws UnknownHostException
    {
        Multimap<InetAddress, InetAddress> broadcastPeers = HashMultimap.create();
        for (int i = 0; i < 4; i ++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, treeRoot);
        }

        Collection<LoadEstimate> estimates = ThicketService.localLoadEstimate(broadcastPeers);
        Assert.assertTrue(estimates.isEmpty());
    }

    @Test
    public void localLoadEstimate_OneInterior() throws UnknownHostException
    {
        Multimap<InetAddress, InetAddress> broadcastPeers = HashMultimap.create();
        for (int i = 0; i < 4; i ++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.1." + i);
            broadcastPeers.put(treeRoot, treeRoot);
        }

        InetAddress treeRoot = InetAddress.getByName("127.1.1.0");
        broadcastPeers.put(treeRoot, treeRoot);
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.1.1.1"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.1.1.2"));

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
        thicket.filterMissingMessages(summary, seen);
        Assert.assertTrue(summary.isEmpty());
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
        thicket.filterMissingMessages(summary, seen);
        Assert.assertFalse(summary.isEmpty());
        Collection<GossipMessageId> gossipMessageIds = summary.get(treeRoot);
        Assert.assertEquals(1, gossipMessageIds.size());
    }

    @Test
    public void addToMissingMessages_EmptyExisting() throws UnknownHostException
    {
        ThicketService thicket = createService(1);
        InetAddress treeRoot = InetAddress.getByName("127.97.21.1");
        GossipMessageId messageId = idGenerator.generate();

        Multimap<InetAddress, GossipMessageId> reportedMissing = HashMultimap.create();
        reportedMissing.put(treeRoot, messageId);

        InetAddress sender = InetAddress.getByName("127.87.12.221");
        List<MissingMessges> missing = new LinkedList<>();
        thicket.addToMissingMessages(sender, missing, reportedMissing);

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
        ThicketService thicket = createService(1);
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

        thicket.addToMissingMessages(sender, existingMissing, reportedMissing);

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

        thicket.handleGraft(new GraftMessage(sender, idGenerator.generate(), roots, Collections.emptyList()));

        Multimap<InetAddress, InetAddress> broadcastPeers = thicket.getBroadcastPeers();
        Assert.assertTrue(broadcastPeers.containsEntry(treeRoot1, sender));
        Assert.assertTrue(broadcastPeers.containsEntry(treeRoot2, sender));
        Assert.assertTrue(((TestMessageSender) thicket.messageSender).messages.isEmpty());
    }

    @Test
    public void handleGraft_OverMaxLoad() throws UnknownHostException
    {
        ThicketService thicket = createService(1);
        InetAddress sender = thicket.getBackupPeers().iterator().next();
        Multimap<InetAddress, InetAddress> broadcastPeers = thicket.getBroadcastPeers();
        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.77"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.78"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.79"));

        InetAddress treeRoot1 = InetAddress.getByName("127.1.1.1");
        InetAddress treeRoot2 = InetAddress.getByName("127.1.1.2");
        List<InetAddress> roots = new LinkedList<InetAddress>() {{ add(treeRoot1); add(treeRoot2); }};

        thicket.handleGraft(new GraftMessage(sender, idGenerator.generate(), roots, Collections.emptyList()));

        broadcastPeers = thicket.getBroadcastPeers();
        Assert.assertFalse(broadcastPeers.containsEntry(treeRoot1, sender));
        Assert.assertFalse(broadcastPeers.containsEntry(treeRoot2, sender));
        Assert.assertFalse(((TestMessageSender) thicket.messageSender).messages.isEmpty());
    }

    @Test
    public void handlePrune() throws UnknownHostException
    {
        ThicketService thicket = createService(1);
        Multimap<InetAddress, InetAddress> broadcastPeers = thicket.getBroadcastPeers();
        InetAddress treeRoot = InetAddress.getByName("127.123.234.10");
        InetAddress sender = InetAddress.getByName("127.123.234.77");
        broadcastPeers.put(treeRoot, sender);
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.78"));
        broadcastPeers.put(treeRoot, InetAddress.getByName("127.123.234.79"));

        Assert.assertTrue(broadcastPeers.containsEntry(treeRoot, sender));
        thicket.handlePrune(new PruneMessage(sender, idGenerator.generate(), new LinkedList<InetAddress>() {{ add(treeRoot); }}, Collections.emptyList()));
        broadcastPeers = thicket.getBroadcastPeers();
        Assert.assertFalse(broadcastPeers.containsEntry(treeRoot, sender));
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
