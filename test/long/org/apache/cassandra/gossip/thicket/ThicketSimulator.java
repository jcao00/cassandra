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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gossip.hyparview.HyParViewSimulator;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

public class ThicketSimulator
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketSimulator.class);
    private TimesSquareDispacher dispatcher;
    private AtomicInteger messagePayload;

    @Before
    public void setup()
    {
        messagePayload = new AtomicInteger();
    }

    @After
    public void tearDown()
    {
        if (dispatcher != null)
            dispatcher.shutdown();
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void basicRun() throws UnknownHostException
    {
        System.out.println("***** thicket simulation - establish peer sampling service *****");
        dispatcher = new TimesSquareDispacher(false);
        MessagingService.instance().addMessageSink(dispatcher);
        HyParViewSimulator.executeCluster(dispatcher, new int[] { 6 });
        stopThickets(dispatcher);
        dispatcher.awaitQuiesence();
        HyParViewSimulator.assertCluster(dispatcher);
        dispatcher.dumpCurrentState();
        restartThickets(dispatcher);

        System.out.println("***** thicket simulation - broadcasting messages *****");
        for (int i = 0; i < 1; i++)
            broadcastMessages(dispatcher, 1);
//        broadcastMessages(dispatcher, 1);

        int limit = 4;
        int count = 0;
        for (InetAddress peer : dispatcher.getPeers())
        {
            TimesSquareDispacher.BroadcastNodeContext context = dispatcher.getContext(peer);
            broadcastMessage(context);
            dispatcher.awaitQuiesence();
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            System.out.println("***** thicket simulation - assert trees *****");
            assertBroadcastTree(dispatcher);
            System.out.println("thicket simulation - complete!");
            count ++;
            if (count == limit)
                break;
        }

        TimesSquareDispacher.BroadcastNodeContext cxt = dispatcher.selectRandom();
        TimesSquareDispacher.BroadcastNodeContext cxt2 = dispatcher.selectRandom();
        for (int i = 0; i < 10; i++)
        {
            broadcastMessage(cxt);
            dispatcher.awaitQuiesence();
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            broadcastMessage(cxt2);
            dispatcher.awaitQuiesence();
            Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

            System.out.println("***** thicket simulation - assert trees *****");
            assertBroadcastTree(dispatcher);
            System.out.println("thicket simulation - complete!");
        }
        dispatcher.awaitQuiesence();

        // give the broadcast trees time to GRAFT, PRUNE, and so on
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);

        System.out.println("***** thicket simulation - assert trees *****");
//        assertBroadcastTree(dispatcher, sentMessages);

        System.out.println("thicket simulation - complete!");
    }

    private void broadcastMessages(TimesSquareDispacher dispatcher, int msgCount)
    {
        for (int i = 0; i < msgCount; i++)
            broadcastMessage(dispatcher.selectRandom());

        dispatcher.awaitQuiesence();

        // give the broadcast trees time to GRAFT, PRUNE, and so on
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    }

    private void broadcastMessage(TimesSquareDispacher.BroadcastNodeContext ctx)
    {
        ctx.broadcastUpdate();
    }

    private static void assertBroadcastTree(TimesSquareDispacher dispatcher)
    {
        stopThickets(dispatcher);

        for (InetAddress addr : dispatcher.getPeers())
        {
            TimesSquareDispacher.BroadcastNodeContext ctx = dispatcher.getContext(addr);
            dumpTree(dispatcher, ctx.thicketService);
            assertFullCoverage(ctx.thicketService, dispatcher);
        }

        restartThickets(dispatcher);
    }

    private static void assertFullCoverage(ThicketService thicket, TimesSquareDispacher dispatcher)
    {
        InetAddress treeRoot = thicket.getLocalAddress();

        // only bother exectuing this check if the ever sent a message (and it was a tree-root)
        if (thicket.getBroadcastedMessageCount() == 0)
            return;

        // for each tree-root, ensure full tree coverage
        Set<InetAddress> completePeers = new HashSet<>(dispatcher.getPeers());

        // put into the queue each child branch Thicket instance plus it's parent
        Queue<Pair<ThicketService, InetAddress>>queue = new LinkedList<>();
        queue.add(Pair.create(thicket, null));

        while (!queue.isEmpty())
        {
            Pair<ThicketService, InetAddress> pair = queue.poll();
            ThicketService svc = pair.left;
            completePeers.remove(svc.getLocalAddress());

            ThicketService.BroadcastPeers broadcastPeers = svc.getBroadcastPeers().get(treeRoot);
            if (broadcastPeers == null)
            {
                logger.error(String.format("%s does not have any peers for treeRoot %s, parent = %s", svc.getLocalAddress(), treeRoot, pair.right));
                continue;
            }

            // check if the instance (in the tree) has an active reference to the parent
            if (pair.right != null && !broadcastPeers.active.contains(pair.right))
                 logger.error(String.format("%s does not have a reference to it's parent (%s) in it's active peers", svc.getLocalAddress(), pair.right));

            // bail out if we have a fully connected mesh
            if (completePeers.isEmpty())
                return;

            for (InetAddress peer : broadcastPeers.active)
            {
                ThicketService branch = dispatcher.getContext(peer).thicketService;

                if (completePeers.contains(branch.getLocalAddress()))
                    queue.add(Pair.create(branch, svc.getLocalAddress()));
            }
        }

        // TODO:JEB someday make this an assert that fails - using for information now
        logger.error(String.format("%s cannot reach the following peers %s", thicket.getLocalAddress(), completePeers));
    }

    private static void dumpTree(TimesSquareDispacher dispatcher, ThicketService thicket)
    {
        if (thicket.getBroadcastedMessageCount() == 0)
            return;
        logger.info(String.format("*** tree peers for %s ***", thicket.getLocalAddress()));
        for (InetAddress addr : dispatcher.getPeers())
        {
            TimesSquareDispacher.BroadcastNodeContext context = dispatcher.getContext(addr);
            ThicketService.BroadcastPeers broadcastPeers = context.thicketService.getBroadcastPeers().get(thicket.getLocalAddress());
            if (broadcastPeers == null)
                logger.info(String.format("\t\t%s - no peers in tree", context.thicketService.getLocalAddress()));
            else
                logger.info(String.format("\t\t%s %s", context.thicketService.getLocalAddress(), broadcastPeers));
        }
    }

    private static void stopThickets(TimesSquareDispacher dispatcher)
    {
        for (InetAddress addr : dispatcher.getPeers())
            dispatcher.getContext(addr).thicketService.testingPause();
    }

    private static void restartThickets(TimesSquareDispacher dispatcher)
    {
        for (InetAddress addr : dispatcher.getPeers())
            dispatcher.getContext(addr).thicketService.testingRestart();
    }
}
