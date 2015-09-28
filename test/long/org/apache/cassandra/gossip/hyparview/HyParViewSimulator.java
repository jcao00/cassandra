package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HyParViewSimulator
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewSimulator.class);

    private static final String LOCAL_DC = "local_dc";
    private static final String REMOTE_DC_1 = "remote_dc_1";
    private static final String REMOTE_DC_2 = "remote_dc_2";

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
        List<InetAddress> seeds = new ArrayList<InetAddress>() {{ add(seed); }};
        dispatcher = new PennStationDispatcher(seeds, false);
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
    public void run_largeCluster_SingleDc() throws UnknownHostException
    {
        for (int i : new int[]{2, 4, 6, 8, 15, 17, 21, 27, 39, 50, 64, 87, 100, 200, 250, 500, 750, 1000, 2000})
        {
            for (int j = 0; j < 64; j++)
            {
                try
                {
                    long start = System.nanoTime();
                    largeCluster_SingleDc(i, false);
                    dispatcher.shutdown();
                    long end = System.nanoTime();
                    logger.info("\titeration {} on cluster size {}; time taken: {}ms, msgs sent: {}",
                                j, i, TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS), dispatcher.totalMessagesSent());
                }
                catch(Throwable e)
                {
                    logger.error("test run failed" , e);
                    Assert.fail(e.getMessage());
                }
            }
        }
    }

    void largeCluster_SingleDc(int clusterSize, boolean verbose) throws UnknownHostException
    {
        InetAddress seedDc1 = InetAddress.getByName("127.0.0.0");
        InetAddress seedDc2 = InetAddress.getByName("127.0.0.1");
        List<InetAddress> seeds = new ArrayList<InetAddress>() {{ add(seedDc1); add(seedDc2); }};
        dispatcher = new PennStationDispatcher(seeds, verbose);

        // add all the nodes to the dispatcher
        for (int i = 0; i < clusterSize; i++)
        {
            int thirdOctet = i / 256;
            int fourthOctet = i % 256;
            InetAddress addr = InetAddress.getByName(String.format("127.0.%d.%d", thirdOctet, fourthOctet));
            dispatcher.addPeer(addr, LOCAL_DC);
            dispatcher.getPeerService(addr).join();
        }

        dispatcher.awaitQuiesence();

        for (int i = 0; i < clusterSize; i++)
        {
            int thirdOctet = i / 256;
            int fourthOctet = i % 256;
            InetAddress node = InetAddress.getByName(String.format("127.0.%d.%d", thirdOctet, fourthOctet));
            HyParViewService hpvService = dispatcher.getPeerService(node);

            // ensure basic sanity -- make sure no peers in remote view
            Assert.assertTrue(hpvService.getRemoteView().isEmpty());
            Assert.assertFalse(hpvService.getLocalDatacenterView().isEmpty());

            // assert symmetric connections
            for (InetAddress peer : hpvService.getLocalDatacenterView())
            {
                HyParViewService peerService = dispatcher.getPeerService(peer);
                Assert.assertTrue(String.format("%s not contained in %s", node, peerService),
                                  peerService.getLocalDatacenterView().contains(node));
            }
        }
    }

    @Test
    @Ignore
    public void largeCluster_MultiDc() throws UnknownHostException
    {
        InetAddress seedDc1 = InetAddress.getByName("127.0.0.0");
        InetAddress seedDc2 = InetAddress.getByName("127.0.1.0");
        List<InetAddress> seeds = new ArrayList<InetAddress>() {{ add(seedDc1); add(seedDc2); }};
        dispatcher = new PennStationDispatcher(seeds, false);

        int dc1Size = 1;
        int dc2Size = 1;

        // add all the nodes to the dispatcher
        for (int i = 0; i < dc1Size; i++)
            dispatcher.addPeer(InetAddress.getByName("127.0.0." + i), REMOTE_DC_1);
        for (int i = 0; i < dc2Size; i++)
            dispatcher.addPeer(InetAddress.getByName("127.0.1." + i), REMOTE_DC_2);

        // now, start the real action!
        for (int i = 1; i < dc1Size; i++)
            dispatcher.getPeerService(InetAddress.getByName("127.0.0." + i)).join();
        for (int i = 1; i < dc2Size; i++)
            dispatcher.getPeerService(InetAddress.getByName("127.0.1." + i)).join();

        dispatcher.awaitQuiesence();
//        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
//        dispatcher.dumpCurrentState();

        // ensure basic sanity -- make sure 0..1 peers in remote view

        // as dc1 is smaller, every node should have a dc2 node in it's view
        Set<InetAddress> otherDcPeers = new HashSet<>();
        int countWithOtherDcInView = 0;
        for (int i = 0; i < dc1Size; i++)
        {
            InetAddress peer = dispatcher.getPeerService(InetAddress.getByName("127.0.0." + i)).getRemoteView().get(REMOTE_DC_2);
            if (peer != null)
            {
                otherDcPeers.add(peer);
                countWithOtherDcInView++;
            }
        }
        // ensure no duplicates
        Assert.assertEquals(dc1Size, countWithOtherDcInView);
        Assert.assertEquals(dc1Size, otherDcPeers.size());

        // as dc2 is larger, each peer from dc1 should only appear once
//        countWithOtherDcInView = 0;
//        for (int i = 0; i < dc2Size; i++)
//            if (dispatcher.getPeerService(InetAddress.getByName("127.0.1." + i)).getRemoteView().containsKey(REMOTE_DC_1))
//                countWithOtherDcInView++;
//        Assert.assertEquals(dc1Size, countWithOtherDcInView);



    }
}
