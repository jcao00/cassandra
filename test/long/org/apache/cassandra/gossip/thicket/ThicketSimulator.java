package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gossip.hyparview.HyParViewSimulator;

public class ThicketSimulator
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketSimulator.class);
    TimesSquareDispacher dispatcher;
    AtomicInteger messagePayload;
    Multimap<InetAddress, Object> sentMessages;

    @Before
    public void setup()
    {
        messagePayload = new AtomicInteger();
        sentMessages = HashMultimap.create();
    }

    @After
    public void tearDown()
    {
        if (dispatcher != null)
            dispatcher.shutdown();
    }

    @Test
    public void basicRun() throws UnknownHostException
    {
        System.out.println("thicket simulation - establish peer sampling service");
        dispatcher = new TimesSquareDispacher(true);
        HyParViewSimulator.executeCluster(dispatcher, new int[] { 6 });
        HyParViewSimulator.assertCluster(dispatcher);

        System.out.println("thicket simulation - broadcasting messages");
        for (int i = 0; i < 1; i++)
            broadcastMessages(dispatcher);

        System.out.println("thicket simulation - assert trees");
        assertBroadcastTree(dispatcher, sentMessages);

        System.out.println("thicket simulation - complete!");
    }

    private void broadcastMessages(TimesSquareDispacher dispatcher)
    {
        for (int i = 0; i < 2; i++)
        {
            TimesSquareDispacher.BroadcastNodeContext ctx = dispatcher.selectRandom();
            String payload = String.valueOf(messagePayload.getAndIncrement());
            ctx.thicketService.broadcast(payload, ctx.client);
            sentMessages.put(ctx.thicketService.getLocalAddress(), payload);
        }

        dispatcher.awaitQuiesence();

        // give the broadcast trees time to GRAFT, PRUNE, and so on
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }

    private static void assertBroadcastTree(TimesSquareDispacher dispatcher, Multimap<InetAddress, Object> sentMessages)
    {
        for (InetAddress addr : dispatcher.getPeers())
        {
            TimesSquareDispacher.BroadcastNodeContext ctx = dispatcher.getContext(addr);
            assertFullCoverage(ctx.thicketService, dispatcher);
            assertAllMessagesReceived(ctx, sentMessages);
        }
    }

    private static void assertFullCoverage(ThicketService thicket, TimesSquareDispacher dispatcher)
    {
        logger.info(String.format("%s broadcast peers: %s", thicket.getLocalAddress(), thicket.getBroadcastPeers()));

        // for each tree-root, ensure full tree coverage
        Set<InetAddress> completePeers = new HashSet<>(dispatcher.getPeers());
        Queue<ThicketService> queue = new LinkedList<>();
        queue.add(thicket);

        while (!queue.isEmpty())
        {
            ThicketService svc = queue.poll();
            completePeers.remove(svc.getLocalAddress());
            // bail out if we have a fully connected mesh
            if (completePeers.isEmpty())
                return;

            for (InetAddress peer : svc.getBroadcastPeers().get(thicket.getLocalAddress()))
            {
                ThicketService branch = dispatcher.getContext(peer).thicketService;

                if (completePeers.contains(branch.getLocalAddress()))
                    queue.add(branch);
            }
        }

        // TODO:JEB someday make this an assert that fails - using for information now
        if (thicket.getBroadcastedMessageCount() > 0)
            logger.error(String.format("%s cannot reach the following peers %s", thicket.getLocalAddress(), completePeers));
    }

    private static void assertAllMessagesReceived(TimesSquareDispacher.BroadcastNodeContext ctx, Multimap<InetAddress, Object> sentMessages)
    {
        int receivedSize = ((TimesSquareDispacher.SimpleClient) ctx.client).received.size();
        if (receivedSize + ctx.thicketService.getBroadcastedMessageCount() != sentMessages.size())
        {
            logger.error(String.format("%s only recevied %d messages, out of a toal of %d",
                                       ctx.thicketService.getLocalAddress(), receivedSize  + ctx.thicketService.getBroadcastedMessageCount(), sentMessages.size()));
            return;
        }

        for (Map.Entry<InetAddress, Collection<Object>> entry : sentMessages.asMap().entrySet())
        {
            for (Object payload : entry.getValue())
            {
                if (entry.getKey().equals(ctx.thicketService.getLocalAddress()))
                    continue;
                if (ctx.client.receive(payload.toString()))
                {
                    logger.info(String.format("%s is missing from tree-root %s value %s",
                                              ctx.thicketService.getLocalAddress(), entry.getKey(), payload));
//                  Assert.assertFalse();
                }
            }
        }
    }
}
