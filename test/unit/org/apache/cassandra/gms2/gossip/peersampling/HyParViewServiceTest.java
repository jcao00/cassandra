package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HyParViewServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewServiceTest.class);

    HyParViewService createService(String ipAddr, List<InetAddress> seeds, PennStationDispatcher dispatcher) throws UnknownHostException
    {
        final InetAddress addr = InetAddress.getByName(ipAddr);
        return createService(addr, seeds, dispatcher);
    }

    HyParViewService createService(InetAddress addr, List<InetAddress> seeds, PennStationDispatcher dispatcher) throws UnknownHostException
    {
        HPVConfigImpl config = new HPVConfigImpl(addr, seeds);
        HyParViewService svc = new HyParViewService(config, dispatcher);
        dispatcher.register(addr, svc);
        return svc;
    }

    @Test
    public void joinSimple() throws UnknownHostException
    {
        PennStationDispatcher dispatcher = new PennStationDispatcher();
        List<InetAddress> seeds = new ArrayList<InetAddress>()
        {{
            add(InetAddress.getByName("127.0.0.1"));
        }};
        HyParViewService seed = createService("127.0.0.1", seeds, dispatcher);
        seed.join();

        waitForQuiesence(dispatcher);
        Assert.assertTrue(seed.getActiveView().isEmpty());
        Assert.assertTrue(seed.getPassiveView().isEmpty());

        HyParViewService peer = createService("127.0.0.2", seeds, dispatcher);
        peer.join();
        waitForQuiesence(dispatcher);

        Assert.assertEquals(1, seed.getActiveView().size());
        Assert.assertTrue(seed.getActiveView().contains(peer.getConfig().getLocalAddr()));
        Assert.assertTrue(seed.getPassiveView().isEmpty());

        Assert.assertEquals(1, peer.getActiveView().size());
        Assert.assertTrue(peer.getActiveView().contains(seed.getConfig().getLocalAddr()));
        Assert.assertTrue(peer.getPassiveView().isEmpty());
    }

    private void waitForQuiesence(PennStationDispatcher dispatcher)
    {
        while (dispatcher.stillWorking())
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

        logger.info("**** quiesent (maybe) ***");
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        logger.info("**** quiesent (done now) ***");
    }

    @Test
    @Ignore
    public void joinManyReally() throws UnknownHostException
    {
        for (int i = 0; i < 128; i++)
        {
            logger.info("\n****************************** \n********** round {} ***********\n******************************", i);
            joinMany();
        }
    }

    public void joinMany() throws UnknownHostException
    {
        PennStationDispatcher dispatcher = buildCluster(1, 6);
        waitForQuiesence(dispatcher);
        ConcurrentHashMap<InetAddress, HyParViewService> nodes = dispatcher.getNodes();
        logger.info("**** new graph ***\n{}", nodes);
        for (Map.Entry<InetAddress, HyParViewService> entry : nodes.entrySet())
        {
            for (InetAddress peerAddr : entry.getValue().getActiveView())
            {
                HyParViewService peer = nodes.get(peerAddr);
                Assert.assertTrue(String.format("node %s is missing reciprocating entry in %s", entry.getValue(), peer),
                                  peer.getActiveView().contains(entry.getKey()));
            }
        }
    }

    public PennStationDispatcher buildCluster(int seedCnt, int peerCnt) throws UnknownHostException
    {
        PennStationDispatcher dispatcher = new PennStationDispatcher();

        List<InetAddress> seeds = new ArrayList<>();
        List<HyParViewService> seedSvcs = new ArrayList<>(seedCnt);
        for (int i = 0; i < seedCnt; i++)
        {
            String ip = String.format("127.0.%d.0", i);
            InetAddress addr = InetAddress.getByName(ip);
            seeds.add(addr);
            HyParViewService seed = createService(addr, seeds, dispatcher);
            seedSvcs.add(seed);
        }

        for (HyParViewService svc : seedSvcs)
            svc.join();

        List<HyParViewService> peerSvcs = new ArrayList<>(peerCnt);
        int nodesPerDC = peerCnt / seedCnt;
        for (int i = 0; i < seedCnt; i++)
        {
            for (int j = 0; j < nodesPerDC; j++)
            {
                HyParViewService peer;
                if (j == 0)
                {
                    // we've already created the seed service instance, don't recreate
                    peer = seedSvcs.get(i);
                }
                else
                {
                    String ip = String.format("127.0.%d.%d", i, j);
                    peer = createService(ip, seeds, dispatcher);
                }
                peer.join();
                peerSvcs.add(peer);
            }
        }

        return dispatcher;
    }

    @Test
    public void buildShuffleList_EmptyViews() throws UnknownHostException
    {
        PennStationDispatcher dispatcher = new PennStationDispatcher();
        List<InetAddress> seeds = new ArrayList<InetAddress>()
        {{
                add(InetAddress.getByName("127.0.0.1"));
            }};
        HyParViewService peer = createService("127.0.0.1", seeds, dispatcher);
        List<InetAddress> activeView = new ArrayList<>();
        List<InetAddress> passiveView = new ArrayList<>();
        Collection<InetAddress> nodes = peer.buildShuffleGroup(activeView, passiveView);
        Assert.assertEquals(1, nodes.size());
    }

    @Test
    public void buildShuffleList_PopulatedViews() throws UnknownHostException
    {
        PennStationDispatcher dispatcher = new PennStationDispatcher();
        List<InetAddress> seeds = new ArrayList<InetAddress>()
        {{
                add(InetAddress.getByName("127.0.0.1"));
            }};
        HyParViewService peer = createService("127.0.0.1", seeds, dispatcher);
        List<InetAddress> activeView = new ArrayList<>();
        activeView.add(InetAddress.getByName("127.0.0.10"));
        activeView.add(InetAddress.getByName("127.0.0.11"));
        List<InetAddress> passiveView = new ArrayList<>();
        passiveView.add(InetAddress.getByName("127.0.1.1"));
        passiveView.add(InetAddress.getByName("127.0.1.2"));
        passiveView.add(InetAddress.getByName("127.0.1.3"));
        passiveView.add(InetAddress.getByName("127.0.1.4"));
        Collection<InetAddress> nodes = peer.buildShuffleGroup(activeView, passiveView);

        int expectedActiveViewCnt = Math.min(activeView.size(), peer.getConfig().getActiveViewLength());
        int expectedPassiveViewCnt = Math.min(passiveView.size(), peer.getConfig().getPassiveViewLength());

        Assert.assertEquals(1 + expectedActiveViewCnt + expectedPassiveViewCnt, nodes.size());
    }

    @Test
    /**
     * Note: we should not get dupes between the active and passive views, but test it out anyways
     */
    public void buildShuffleList_PopulatedViewsWithDupes() throws UnknownHostException
    {
        PennStationDispatcher dispatcher = new PennStationDispatcher();
        List<InetAddress> seeds = new ArrayList<InetAddress>()
        {{
            add(InetAddress.getByName("127.0.0.1"));
        }};
        HyParViewService peer = createService("127.0.0.1", seeds, dispatcher);
        List<InetAddress> activeView = new ArrayList<>();
        activeView.add(InetAddress.getByName("127.0.0.10"));
        activeView.add(InetAddress.getByName("127.0.0.11"));
        List<InetAddress> passiveView = new ArrayList<>();
        passiveView.add(InetAddress.getByName("127.0.0.10"));
        passiveView.add(InetAddress.getByName("127.0.0.11"));
        Collection<InetAddress> nodes = peer.buildShuffleGroup(activeView, passiveView);

        int expectedActiveViewCnt = Math.min(activeView.size(), peer.getConfig().getActiveViewLength());
        Assert.assertEquals(String.format("expected count: %d, returned list: %s", (1 + expectedActiveViewCnt), nodes), 1 + expectedActiveViewCnt, nodes.size());
    }

    @Test
    public void applyShuffle() throws UnknownHostException
    {
        // boiletplate of building up a node
        PennStationDispatcher dispatcher = new PennStationDispatcher();
        List<InetAddress> seeds = new ArrayList<InetAddress>()
        {{
            add(InetAddress.getByName("127.0.0.1"));
        }};
        HyParViewService node = createService("127.0.0.1", seeds, dispatcher);
        List<InetAddress> activeView = new ArrayList<>();
        activeView.add(InetAddress.getByName("127.0.0.2"));
        node.addToActiveView(activeView);

        List<InetAddress> passiveView = new ArrayList<>();
        passiveView.add(InetAddress.getByName("127.0.0.10"));
        passiveView.add(InetAddress.getByName("127.0.0.11"));
        node.addToPassiveView(passiveView);

        Assert.assertEquals(String.format("passive view before apply: %s", node.getPassiveView()),
                            passiveView.size(), node.getPassiveView().size());

        List<InetAddress> shuffledNodes = new ArrayList<>();
        shuffledNodes.add(passiveView.get(0));
        shuffledNodes.add(InetAddress.getByName("127.0.0.12"));

        List<InetAddress> previouslySent = new ArrayList<>();
        previouslySent.add(InetAddress.getByName("127.0.0.11"));
        previouslySent.add(InetAddress.getByName("127.0.0.21"));

        node.applyShuffle(shuffledNodes, previouslySent);
        Assert.assertEquals(String.format("passive view after apply: %s", node.getPassiveView()),
                            3, node.getPassiveView().size());
    }
}
