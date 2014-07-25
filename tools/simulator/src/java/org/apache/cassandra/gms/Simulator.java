package org.apache.cassandra.gms;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class Simulator
{
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);
    private CustomMessagingService customMessagingService;

    public static void main(String[] args) throws Exception
    {
        String cwd = System.getProperty("user.dir");
        System.setProperty("logback.configurationFile", "src/main/resources/logback.xml");
        System.setProperty("cassandra.config", "file://" + cwd + "/src/main/resources/cassandra.yaml");

        Simulator simulator = new Simulator();
        simulator.runSimulation(1, 3, 10);
//        simulator.runSimulation(3, 25, 10);
//        simulator.runSimulation(3, 50, 10);
//        simulator.runSimulation(3, 100, 10);
//        simulator.runSimulation(6, 200, 10);
//        simulator.runSimulation(6, 400, 10);
//        simulator.runSimulation(12, 800, 10);
//        simulator.runSimulation(20, 1200, 10);

        System.exit(0);
    }

    void runSimulation(int seedCnt, int nodeCnt, int simulationRounds)
    {
        logger.warn("####### Running new simulation for {} nodes with {} seeds ######", nodeCnt, seedCnt);

        for (int i = 0; i < simulationRounds; i ++)
            runSimulation(seedCnt, nodeCnt);
    }

    void runSimulation(int seedCnt, int nodeCnt)
    {
        assert seedCnt > nodeCnt;
        customMessagingService = new CustomMessagingService();

        List<InetAddress> seeds = new ArrayList<>(seedCnt);
        for (int i = 0; i < seedCnt; i++)
        {
            seeds.add(getInetAddr(i));
        }

        CountDownLatch latch = new CountDownLatch(1);
        CyclicBarrier barrier = new CyclicBarrier(nodeCnt, new BarrierAction(latch));

        for (int i = 0; i < nodeCnt; i++)
        {
            InetAddress addr = getInetAddr(i);
            IPartitioner partitioner = new Murmur3Partitioner();
            CustomGossiper gossiper = new CustomGossiper();
            gossiper.setBarrier(barrier);
            PeerStatusService peerStatusService = new PeerStatusService(gossiper, partitioner, false);
            Gossiper simulator = peerStatusService.gossiper;
            customMessagingService.register(addr, simulator);

            Map<ApplicationState, VersionedValue> appStates = new HashMap<>();
            appStates.put(ApplicationState.NET_VERSION, peerStatusService.versionedValueFactory.networkVersion());
            appStates.put(ApplicationState.HOST_ID, peerStatusService.versionedValueFactory.hostId(UUID.randomUUID()));
            appStates.put(ApplicationState.RPC_ADDRESS, peerStatusService.versionedValueFactory.rpcaddress(addr));
            appStates.put(ApplicationState.RELEASE_VERSION, peerStatusService.versionedValueFactory.releaseVersion());
            appStates.put(ApplicationState.DC, peerStatusService.versionedValueFactory.datacenter("dc" + (i % 2)));
            appStates.put(ApplicationState.RACK, peerStatusService.versionedValueFactory.rack("rack" + (i % 3)));

            Collection<Token> localTokens = new ArrayList<>();
            for (int j = 0; j < 3; j++)
                localTokens.add(partitioner.getRandomToken());
            appStates.put(ApplicationState.TOKENS, peerStatusService.versionedValueFactory.tokens(localTokens));
            appStates.put(ApplicationState.STATUS, peerStatusService.versionedValueFactory.normal(localTokens));

            // some random generation value (a/k/a timestamp of last app launch)
            int gen = (int)(System.currentTimeMillis() / 1000) - (int)(1000 * Math.random());
            simulator.start(gen, appStates);
        }

        try
        {
            latch.await(10, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            logger.error("test with {} seeds and {} nodes timed out before completion", seedCnt, nodeCnt);
        }

        //shut down everything - might be some noisy errors?
        for (Gossiper gossiper : customMessagingService.gossipers.values())
        {
            gossiper.terminate();
        }

        // wait a short while for things to die
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            //nop
        }
    }

    InetAddress getInetAddr(int i)
    {
        int thirdOctet = i / 255;
        int fourthOctet = i % 255;
        String ipAddr = "127.0." + thirdOctet + "." + fourthOctet;
        try
        {
            return InetAddress.getByName(ipAddr);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException("couldn't translate name to ip addr, input = " + i, e);
        }
    }

    class BarrierAction implements Runnable
    {
        int counter = 0;
        int lastConvergenceRound = 0;
        private final CountDownLatch latch;

        public BarrierAction(CountDownLatch latch)
        {
            this.latch = latch;
        }

        public void run()
        {
            logger.debug("**************** ROUND {}  **************************", counter);
            counter++;
            if (counter <= 1)
            {
                logger.debug("****** skipping initial round on convergence checking");
                return;
            }

            long start = System.currentTimeMillis();
//            boolean convergedViaGossip = hasConvergedViaGossip();
            boolean convergedByInspection = hasConvergedByInspection();
            logger.debug("****** elapsed comparison time (ms) = " + (System.currentTimeMillis() - start));
            logger.debug("****** have we converged? " + convergedByInspection);

            if (convergedByInspection)
            {
                if (counter - 1 > lastConvergenceRound)
                    logger.warn("****** converged after {} rounds", (counter - lastConvergenceRound));

                lastConvergenceRound = counter;

                latch.countDown();
            }
            else
                logger.debug("****** rounds since convergence = {} ", (counter - lastConvergenceRound));

            // TODO: execute new behaviors: add/remove node (and replace barrier in simulators) and other actions
            // bounce nodes
        }

        boolean hasConvergedViaGossip()
        {
            // hoping like hell there's a less miserable way to do this diff'ing...
//            Map<InetAddress, Gossiper> gossipers = CustomMessagingService.instance().gossipers;
//            for (Gossiper simulator : gossipers.values())
//            {
//                List<GossipDigest> digests = new ArrayList<>(gossipers.size());
//                simulator.makeRandomGossipDigest(digests);
//                for (GossipDigest digest : digests)
//                {
//                    Gossiper peer = gossipers.get(digest.getEndpoint());
//                    if (null == peer)
//                        return false;
//
//                }
//            }

            return true;
        }

        boolean hasConvergedByInspection()
        {
            Map<InetAddress, Gossiper> gossipers = customMessagingService.gossipers;
            for (Map.Entry<InetAddress, Gossiper> entry : gossipers.entrySet())
            {
                InetAddress localAddr = entry.getKey();
                Gossiper gossiper = entry.getValue();

                Collection<InetAddress> peerAddrs = new ArrayList<>(gossipers.keySet());

                for (Map.Entry<InetAddress, EndpointState> peer : gossiper.endpointStateMap.entrySet())
                {
                    InetAddress peerAddr = peer.getKey();
                    //this case *really* shouldn't fail - would seem to be more of my error than anything else
                    if (!peerAddrs.remove(peerAddr))
                        return false;
                    if (peerAddr.equals(entry.getKey()))
                        continue;

                    // simulator knows about peer, now let's compare states
                    EndpointState localEndpointState = peer.getValue();
                    EndpointState peerEndpointState = gossipers.get(peerAddr).getEndpointStateForEndpoint(peerAddr);

                    // first compare the heartbeats
                    //NOTE: the heartBeat.version is almost guaranteed to be different (non-convergent), especially in anything larger than a very small cluster,
                    // as the target/source node updates it's heartbeat.version on every gossip round. thus, don't bother to compare them
                    if (localEndpointState.getHeartBeatState().getGeneration() != peerEndpointState.getHeartBeatState().getGeneration())
                    {
                        logger.debug("hasConvergedByInspection: generations are different: local = {}, target = {}",
                                localEndpointState.getHeartBeatState().getGeneration(), peerEndpointState.getHeartBeatState().getGeneration());
                        return false;
                    }

                    // next, compare the app states
                    Collection<ApplicationState> peerAppStates = new ArrayList<>(peerEndpointState.applicationState.keySet());
                    for (Map.Entry<ApplicationState, VersionedValue> localAppStateEntry : localEndpointState.applicationState.entrySet())
                    {
                        ApplicationState appState = localAppStateEntry.getKey();
                        if (!peerAppStates.remove(appState))
                        {
                            logger.debug("hasConvergedByInspection: unknown app state: peer {} does not have AppState {} that local does", new Object[]{peerAddr, appState, localAddr});
                            return false;
                        }
                        if (localAppStateEntry.getValue().compareTo(peerEndpointState.getApplicationState(appState)) != 0)
                        {
                            logger.debug("hasConvergedByInspection: divergent app state: AppState {} has local({}) version {} and peer({}) version {}",
                                    new Object[]{appState, localAddr, localAppStateEntry.getValue().value, peerAddr, peerEndpointState.getApplicationState(appState).value});
                            return false;
                        }
                    }
                    if (!peerAppStates.isEmpty())
                    {
                        logger.debug("hasConvergedByInspection: unknown app states: current node {} doesn't know about the following app states from {}: {}", new Object[]{localAddr, peerAddr, peerAppStates});
                        return false;
                    }
                }

                if (!peerAddrs.isEmpty())
                {
                    if (peerAddrs.size() < 8)
                        logger.debug("hasConvergedByInspection: unknown nodes: current node {} doesn't know about the following nodes: {}", localAddr, peerAddrs);
                    else
                        logger.debug("hasConvergedByInspection: unknown nodes: current node {} doesn't know about {} nodes (out of {} total)", new Object[]{localAddr, peerAddrs.size(), gossipers.size()});
                    return false;
                }
            }
            return true;
        }
    }
}
