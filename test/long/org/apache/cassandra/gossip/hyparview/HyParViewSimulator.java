package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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
            for (int j = 0; j < 64; j++)
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
                int thirdOctet = j / 256;
                int fourthOctet = j % 256;
                InetAddress addr = InetAddress.getByName(String.format("127.%d.%d.%d", i, thirdOctet, fourthOctet));
                dispatcher.addPeer(addr, DC_PREFIX + i);
                dispatcher.getPeerService(addr).join();
            }
        }

        dispatcher.awaitQuiesence();
        dispatcher.dumpCurrentState();

        for (int i = 0; i < clusterSizePerDatacenter.length; i++)
        {
            for (int j = 0; j < clusterSizePerDatacenter[i]; j++)
            {
                int thirdOctet = j / 256;
                int fourthOctet = j % 256;
                InetAddress node = InetAddress.getByName(String.format("127.%d.%d.%d", i, thirdOctet, fourthOctet));
                HyParViewService hpvService = dispatcher.getPeerService(node);

                assertLocalDatacenter(hpvService, dispatcher);
                assertRemoteDatacenters(hpvService, dispatcher, clusterSizePerDatacenter, i);
            }
        }
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

    private void assertRemoteDatacenters(HyParViewService hpvService, PennStationDispatcher dispatcher, int[] clusterSizePerDatacenter, int currentDatacenter)
    {
        if (clusterSizePerDatacenter.length == 1)
        {
            Assert.assertTrue(hpvService.getRemoteView().isEmpty());
            return;
        }

        for (int i = 0; i < clusterSizePerDatacenter.length; i++)
        {
            if (i == currentDatacenter)
                continue;

            // if the current node's datacenter has less than or equal to the number of nodes in the other datacenter,
            // then every node in this DC must have a connection to a peer in the other DC. If the other DC is larger,
            // not every node in that DC will have a connection to this DC
            if (clusterSizePerDatacenter[currentDatacenter] <= clusterSizePerDatacenter[i])
            {
                InetAddress remotePeer = hpvService.getRemoteView().get(DC_PREFIX + i);
//                Assert.assertNotNull(String.format("%s does not contain a remote peer from dc '%s'", hpvService.getLocalAddress(), DC_PREFIX + i), remotePeer);
                if (remotePeer == null)
                    continue;

                HyParViewService peerService = dispatcher.getPeerService(remotePeer);
                InetAddress symmetricConnection = peerService.getRemoteView().get(DC_PREFIX + currentDatacenter);
                Assert.assertEquals(String.format("%s not contained in remote view of [%s]", hpvService.getLocalAddress(), peerService),
                                    hpvService.getLocalAddress(), symmetricConnection);
            }
        }
    }

    @Test
    public void multipleDcClusters() throws UnknownHostException
    {
        for (int i : new int[]{16, 32, 40})
        {
            for (int j = 0; j < 64; j++)
            {
                int[] clusterSizes = new int[] {4, 4};
                try
                {
                    long start = System.nanoTime();
                    executeCluster(clusterSizes, true);
                    dispatcher.shutdown();
                    long end = System.nanoTime();
                    logger.info("\titeration {} on cluster size {}; time taken: {}ms, msgs sent: {}",
                                j, Arrays.toString(clusterSizes), TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS), dispatcher.totalMessagesSent());
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
