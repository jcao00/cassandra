package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HyParViewSimulator
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewSimulator.class);

    private static final String LOCAL_DC = "local_dc";
    private static final String DC_PREFIX = "dc_";

    PennStationDispatcher dispatcher;

    @Before
    public void setUp()
    {
    }

    @After
    public void tearDown()
    {
        if (dispatcher != null)
            dispatcher.shutdown();
    }

    @Test
    public void simpleJoin_TwoNodes() throws UnknownHostException
    {
        InetAddress seed = InetAddress.getByName("127.0.0.1");
        dispatcher = new PennStationDispatcher(false);
        dispatcher.addSeed(seed);
        dispatcher.addPeer(seed, LOCAL_DC);

        InetAddress peer = InetAddress.getByName("127.0.0.2");
        dispatcher.addPeer(peer, LOCAL_DC);

        // join thw two nodes
        dispatcher.getPeerService(seed).join();
        dispatcher.awaitQuiesence();
        Assert.assertTrue(dispatcher.getPeerService(seed).getPeers().isEmpty());
        Assert.assertTrue(dispatcher.getPeerService(peer).getPeers().isEmpty());

        dispatcher.getPeerService(peer).join();
        dispatcher.awaitQuiesence();

        Assert.assertTrue(dispatcher.getPeerService(seed).getPeers().contains(peer));
        Assert.assertTrue(dispatcher.getPeerService(peer).getPeers().contains(seed));

        // node, disconnect them - they should not attempt to reconnect via neighbor request
        Assert.assertTrue(dispatcher.getPeerService(peer).removePeer(seed, LOCAL_DC));
        dispatcher.awaitQuiesence();
        Assert.assertTrue(dispatcher.getPeerService(peer).getPeers().isEmpty());
        Assert.assertTrue(dispatcher.getPeerService(seed).getPeers().isEmpty());
    }

    @Test
    public void singleDcClusters() throws UnknownHostException
    {
        for (int i : new int[]{2, 4, 6, 8, 15, 17, 21, 27, 39, 50, 64, 87, 100, 200, 250, 500, 750, 1000, 2000})
        {
            for (int j = 0; j < 32; j++)
            {
                try
                {
                    long start = System.nanoTime();
                    executeCluster(new int[]{ i }, false);
                    dispatcher.shutdown();
                    long end = System.nanoTime();
                    logger.info("\titeration {} on cluster size {}; time taken: {}ms, msgs sent: {}",
                                j, i, TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS), dispatcher.totalMessagesSent());
                }
                catch(Throwable e)
                {
                    logger.error("test run failed" , e);
                    throw e;
                }
            }
        }
    }

    void executeCluster(int[] clusterSizePerDatacenter, boolean verbose) throws UnknownHostException
    {
        dispatcher = new PennStationDispatcher(verbose);

        // add all the nodes to the dispatcher
        for (int i = 0; i < clusterSizePerDatacenter.length; i++)
        {
            dispatcher.addSeed(InetAddress.getByName(String.format("127.%d.0.0", i)));
            dispatcher.addSeed(InetAddress.getByName(String.format("127.%d.0.1", i)));

            for (int j = 0; j < clusterSizePerDatacenter[i]; j++)
            {
                InetAddress addr = generateAddr(i, j);
                dispatcher.addPeer(addr, DC_PREFIX + i);
                dispatcher.getPeerService(addr).join();
            }

            // if there's more DCs to process, give the current one a moment to resolve it's own messages
            if (i + i < clusterSizePerDatacenter.length)
                Uninterruptibles.sleepUninterruptibly(clusterSizePerDatacenter[i], TimeUnit.MILLISECONDS);
        }

        dispatcher.awaitQuiesence();

        // simulate a round of gossip to propagate cluster metadata
        for (HyParViewService hpvService : dispatcher.getNodes())
            for (HyParViewService peer : dispatcher.getNodes())
                hpvService.endpointStateSubscriber.add(peer.getLocalAddress(), peer.getDatacenter());

        for (HyParViewService hpvService : dispatcher.getNodes())
            dispatcher.checkActiveView(hpvService.getLocalAddress());

        dispatcher.awaitQuiesence();

        for (HyParViewService hpvService : dispatcher.getNodes())
        {
            assertLocalDatacenter(hpvService, dispatcher);
            assertRemoteDatacenters(hpvService, dispatcher);
            assertCompleteMeshCoverage(hpvService, dispatcher);
        }
    }

    private void assertCompleteMeshCoverage(HyParViewService hpvService, PennStationDispatcher dispatcher)
    {
        Set<InetAddress> completePeers = new HashSet<>(dispatcher.getPeers());
        Queue<HyParViewService> queue = new LinkedList<>();
        queue.add(hpvService);

        while (!queue.isEmpty())
        {
            HyParViewService hpv = queue.poll();
            completePeers.remove(hpv.getLocalAddress());
            // bail out if we have a fully connected mesh
            if (completePeers.size() == 0)
                return;

            for (InetAddress peer : hpv.getPeers())
            {
                HyParViewService peerService = dispatcher.getPeerService(peer);

                if (completePeers.contains(peerService.getLocalAddress()))
                    queue.add(peerService);
            }
        }

        // TODO:JEB someday make this an assert that fails - using for information now
        logger.info(String.format("%s cannot reach the following peers %s", hpvService.getLocalAddress(), completePeers));
    }

    InetAddress generateAddr(int dc, int node) throws UnknownHostException
    {
        int thirdOctet = node / 256;
        int fourthOctet = node % 256;
        return InetAddress.getByName(String.format("127.%d.%d.%d", dc, thirdOctet, fourthOctet));
    }

    private void assertLocalDatacenter(HyParViewService hpvService, PennStationDispatcher dispatcher)
    {
        Assert.assertFalse(String.format("node: %s", hpvService), hpvService.getLocalDatacenterView().isEmpty());

        // assert symmetric connections
        for (InetAddress peer : hpvService.getLocalDatacenterView())
        {
            HyParViewService peerService = dispatcher.getPeerService(peer);
            Assert.assertTrue(String.format("%s not contained in [%s]", hpvService.getLocalAddress(), peerService),
                              peerService.getLocalDatacenterView().contains(hpvService.getLocalAddress()));
        }
    }

    private void assertRemoteDatacenters(HyParViewService hpvService, PennStationDispatcher dispatcher)
    {
        Multimap<String, InetAddress> peers = hpvService.endpointStateSubscriber.getPeers();

        if (peers.size() == 1)
        {
            Assert.assertTrue(hpvService.getRemoteView().isEmpty());
            return;
        }

        Collection<InetAddress> nodesLocalPeers = peers.removeAll(hpvService.getDatacenter());

        for (Map.Entry<String, Collection<InetAddress>> entry : peers.asMap().entrySet())
        {
            // if the current node's datacenter has less than or equal to the number of nodes in the other datacenter,
            // then every node in this DC must have a connection to a peer in the other DC. If the other DC is larger,
            // not every node in that DC will have a connection to this DC
            if (nodesLocalPeers.size() <= entry.getValue().size())
            {
                InetAddress remotePeer = hpvService.getRemoteView().get(entry.getKey());
                if (remotePeer == null)
                    continue;

                HyParViewService peerService = dispatcher.getPeerService(remotePeer);
                InetAddress symmetricConnection = peerService.getRemoteView().get(hpvService.getDatacenter());
                Assert.assertEquals(String.format("%s not contained in remote view of [%s]", hpvService.getLocalAddress(), peerService),
                                    hpvService.getLocalAddress(), symmetricConnection);
            }
        }
    }

    @Test
    public void multipleDcClusters() throws UnknownHostException
    {
        int fanoutThreshold = HyParViewService.EndpointStateSubscriber.NATURAL_LOG_THRESHOLD;
        int[] clusterSizes = { fanoutThreshold - 1, fanoutThreshold, fanoutThreshold + 4};

        for (int dcCount = 1; dcCount < 10; dcCount++)
        {
            for (int i : clusterSizes)
            {
                for (int j : clusterSizes)
                {
                    try
                    {
                        long start = System.nanoTime();

                        int[] dcSizes = new int[dcCount];
                        for (int q = 0; q < dcSizes.length; q++)
                            dcSizes[q] = i;
                        dcSizes[dcSizes.length - 1] = j;
                        logger.info("******* starting next multi-dc run: {} *********", Arrays.toString(dcSizes));

                        executeCluster(dcSizes, false);
                        dispatcher.shutdown();
                        long end = System.nanoTime();
                        logger.info("\ton cluster sizes {}; time taken: {}ms, msgs sent: {}",
                                    Arrays.toString(dcSizes), TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS), dispatcher.totalMessagesSent());
                    }
                    catch(Throwable e)
                    {
                        logger.error("test run failed", e);
                        throw e;
                    }
                }
            }
        }
    }
}
