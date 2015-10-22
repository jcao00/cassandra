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

import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.PeerSamplingService;
import org.apache.cassandra.gossip.PeerSamplingServiceListener;

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
        return createService(peersCount, System.nanoTime());
    }

    private ThicketService createService(int peersCount, long startTime)
    {
        ThicketService thicket = new ThicketService(localNodeAddr, new TestMessageSender(), executorService, scheduler);
        thicket.start(new SimplePeerSamplingService(peersCount), 42, startTime);
        return thicket;
    }

    @Test
    public void selectBroadcastPeers_Empty()
    {
        ThicketService thicket = createService(0);
        ThicketService.BroadcastPeers broadcastPeers = thicket.selectBroadcastPeers(Collections.emptyList(), Optional.<InetAddress>empty(), 5);
        Assert.assertTrue(broadcastPeers.activePeers.isEmpty());
        Assert.assertTrue(broadcastPeers.backupPeers.isEmpty());
    }

    @Test
    public void selectBroadcastPeers_NonTreeRoot_SmallPeersList() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        List<InetAddress> peers = new LinkedList<>();
        int maxSize = 5;
        for (int i = 0; i < (maxSize - 2); i ++)
            peers.add(InetAddress.getByName("127.0.1." + i));

        ThicketService.BroadcastPeers broadcastPeers = thicket.selectBroadcastPeers(peers, Optional.of(InetAddress.getByName("127.0.1.1")), maxSize);
        Assert.assertFalse(broadcastPeers.activePeers.isEmpty());
        Assert.assertTrue(broadcastPeers.backupPeers.isEmpty());
    }

    @Test
    public void selectBroadcastPeers_TreeRoot_SmallPeersList() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        List<InetAddress> peers = new LinkedList<>();
        int maxSize = 5;
        for (int i = 0; i < (maxSize - 2); i ++)
            peers.add(InetAddress.getByName("127.0.1." + i));

        ThicketService.BroadcastPeers broadcastPeers = thicket.selectBroadcastPeers(peers, Optional.of(thicket.getLocalAddress()), maxSize);
        Assert.assertFalse(broadcastPeers.activePeers.isEmpty());
        Assert.assertTrue(broadcastPeers.backupPeers.isEmpty());
    }

    @Test
    public void selectBroadcastPeers_NonTreeRoot_LargePeersList() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        List<InetAddress> peers = new LinkedList<>();
        int maxSize = 5;
        for (int i = 0; i < (maxSize + 2); i ++)
            peers.add(InetAddress.getByName("127.0.1." + i));

        ThicketService.BroadcastPeers broadcastPeers = thicket.selectBroadcastPeers(peers, Optional.of(thicket.getLocalAddress()), maxSize);
        Assert.assertFalse(broadcastPeers.activePeers.isEmpty());
        Assert.assertFalse(broadcastPeers.backupPeers.isEmpty());
        Assert.assertTrue(Collections.disjoint(broadcastPeers.activePeers, broadcastPeers.backupPeers));
    }

    @Test
    public void selectBroadcastPeers_TreeRoot_LargePeersList() throws UnknownHostException
    {
        ThicketService thicket = createService(0);
        List<InetAddress> peers = new LinkedList<>();
        int maxSize = 5;
        for (int i = 0; i < (maxSize + 2); i ++)
            peers.add(InetAddress.getByName("127.0.1." + i));

        ThicketService.BroadcastPeers broadcastPeers = thicket.selectBroadcastPeers(peers, Optional.of(thicket.getLocalAddress()), maxSize);
        Assert.assertFalse(broadcastPeers.activePeers.isEmpty());
        Assert.assertFalse(broadcastPeers.backupPeers.isEmpty());
        Assert.assertTrue(Collections.disjoint(broadcastPeers.activePeers, broadcastPeers.backupPeers));
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
        List<ThicketService.MissingMessges> missing = new LinkedList<>();
        InetAddress treeRoot = InetAddress.getByName("127.0.1.23");
        GossipMessageId messageId = idGenerator.generate();

        ThicketService.MissingMessges msgs = new ThicketService.MissingMessges(42);
        ThicketService.MissingSummary summary = new ThicketService.MissingSummary();
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
        List<ThicketService.MissingMessges> missing = new LinkedList<>();
        InetAddress treeRoot = InetAddress.getByName("127.0.1.23");
        GossipMessageId messageId = idGenerator.generate();

        ThicketService.MissingMessges msgs = new ThicketService.MissingMessges(42);
        ThicketService.MissingSummary summary = new ThicketService.MissingSummary();
        List<GossipMessageId> msgIds = new LinkedList<>();
        msgIds.add(messageId);
        msgIds.add(idGenerator.generate());

        summary.add(InetAddress.getByName("127.8.1.55"), msgIds);
        msgs.trees.put(treeRoot, summary);
        missing.add(msgs);

        ThicketService thicket = createService(1);
        thicket.removeFromMissing(missing, treeRoot, messageId);
        ThicketService.MissingSummary missingSummary = msgs.trees.get(treeRoot);
        Assert.assertNotNull(missingSummary);
        Assert.assertEquals(1, missingSummary.messages.size());
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












    /*
        testing fixtures
     */

    static class SimpleClient implements BroadcastServiceClient
    {
        private final List<String> received = new LinkedList<>();

        public String getClientName()
        {
            return "simple-client";
        }

        public boolean receive(Object payload)
        {
            if (received.contains(payload))
                return false;
            received.add(payload.toString());
            return true;
        }
    }

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
