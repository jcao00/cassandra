package org.apache.cassandra.gms;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;

public class Simulator
{
    private static final Logger logger = LoggerFactory.getLogger(Simulator.class);
    private final int seedCnt;
    private final int nodeCnt;
    private final int simulationRounds;
    private GossipSimulatorDispatcher customMessagingService;
    private static final ExecutorService executor = Executors.newFixedThreadPool(128);

    public static void main(String[] args) throws Exception
    {
        String cwd = System.getProperty("user.dir");
        String path = cwd + "/tools/simulator/src/resources/";
//        System.setProperty("logback.configurationFile", path + "logback.xml");
        System.setProperty("cassandra.config", "file://" + path + "cassandra.yaml");

        if (args.length < 3)
        {
            logger.info("running simulator with default config");
            new Simulator(1, 3, 10).runSimulation();
            new Simulator(3, 25, 10).runSimulation();
            new Simulator(3, 50, 10).runSimulation();
            new Simulator(3, 100, 10).runSimulation();
            new Simulator(6, 200, 10).runSimulation();
            new Simulator(6, 400, 10).runSimulation();
            new Simulator(12, 800, 10).runSimulation();
            new Simulator(20, 1200, 10).runSimulation();
        }
        else
        {
            new Simulator(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2])).runSimulation();
        }

        System.exit(0);
    }
    
    Simulator(int seedCnt, int nodeCnt, int simulationRounds)
    {
        this.seedCnt = seedCnt;
        this.nodeCnt = nodeCnt;
        this.simulationRounds = simulationRounds;
    }

    void runSimulation()
    {
        logger.warn("####### Running new simulation for {} nodes with {} seeds ######", nodeCnt, seedCnt);

        for (int i = 0; i < simulationRounds; i++)
        {
            logger.warn("####### Running simulation round {} ######", i);
            runSimulation(seedCnt, nodeCnt);
        }
    }

    void runSimulation(int seedCnt, int nodeCnt)
    {
        assert seedCnt < nodeCnt;
        final int increment = 32;
        CountDownLatch latch = new CountDownLatch(1);
        BarrierAction action = new BarrierAction(latch);
        CyclicBarrier barrier = new CyclicBarrier(increment, action);
        action.barrier = barrier;
        customMessagingService = new GossipSimulatorDispatcher(barrier);

        List<InetAddress> seeds = new ArrayList<>(seedCnt);
        for (int i = 0; i < seedCnt; i++)
        {
            seeds.add(getInetAddr(i));
        }

        SimulatorSeedProvider.setSeeds(seeds);
        logger.info("****** seeds = {}", seeds);

        for (int i = 0; i < nodeCnt; i++)
        {
            InetAddress addr = getInetAddr(i);
            IPartitioner partitioner = new Murmur3Partitioner();
            PeerStatusService peerStatusService = new PeerStatusService(addr, partitioner, customMessagingService, false);
            Gossiper gossiper = peerStatusService.gossiper;
            customMessagingService.register(addr, gossiper);

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
            int gen = (int) (System.currentTimeMillis() / 1000) - (int) (1000 * Math.random());
            gossiper.start(gen, appStates);

            if (i > 0 && i % increment == 0)
            {
                // wait for convergence
                try
                {
                    logger.info("launched {} instances, waiting for convergence", i);
                    latch.await(1, TimeUnit.MINUTES);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException("instances would not converge with node count = " + i);
                }

                // now update the barrier
                int newCnt = i + increment;
                if (newCnt > nodeCnt)
                    newCnt = nodeCnt;
                latch = new CountDownLatch(1);
                action = new BarrierAction(latch);
                barrier = new CyclicBarrier(newCnt, action);
                customMessagingService.setBarrier(barrier);
            }
        }

//        latch = new CountDownLatch(1);
//        action = new BarrierAction(latch);
//        barrier = new CyclicBarrier(nodeCnt, action);
//        customMessagingService.setBarrier(barrier);
        try
        {
            logger.info("launched {} instances, waiting for convergence", nodeCnt);
            latch.await(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("instances would not converge with node count = " + nodeCnt);
        }

        //shut down everything - might be some noisy errors?
        for (Gossiper gossiper : customMessagingService.gossipers.values())
        {
            gossiper.terminate();
        }

        // wait a short while for things to die
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }

    InetAddress getInetAddr(int i)
    {
        int thirdOctet = i / 255;
        int fourthOctet = i % 255;
        String ipAddr = "127.0." + thirdOctet + "." + fourthOctet;
        try
        {
            return InetAddress.getByName(ipAddr);
        } catch (UnknownHostException e)
        {
            throw new RuntimeException("couldn't translate name to ip addr, input = " + i, e);
        }
    }

    class BarrierAction implements Runnable
    {
        private final CountDownLatch latch;
        CyclicBarrier barrier;
        int counter = 0;
        int lastConvergenceRound = 0;
        private long convergenceTs;
        private String convergedByInspection;

        public BarrierAction(CountDownLatch latch)
        {
            this.latch = latch;
            convergenceTs = -1;
        }

        public void run()
        {
            int curRound = counter++;
            if (curRound == 0)
            {
                logger.debug("**************** Starting simulator **************************");
                return;
            }

            logger.debug("**************** ROUND {} **************************", curRound);
            long start = System.currentTimeMillis();
            convergedByInspection = hasConvergedByInspection();
            long elapseCompTime = System.currentTimeMillis() - start;

            if (50 < elapseCompTime)
                logger.debug("****** elapsed comparison time = {} ms", elapseCompTime);
            logger.debug("****** have we converged? " + (convergedByInspection == null));

            if (convergedByInspection == null)
            {
                if (-1 == convergenceTs)
                {
                    logger.warn("****** converged after {} rounds", (counter - lastConvergenceRound));
                    convergenceTs = System.currentTimeMillis();
                    lastConvergenceRound = curRound;
                }

                // cycle for a few extra rounds to make sure everything did converge (and didn't start dropping out)
//                if (System.currentTimeMillis() - (60 * 1000) > convergenceTs)
                {
                    barrier.reset();
                    latch.countDown();
                }
            }
            else
            {
                logger.debug(convergedByInspection);
                logger.debug("****** rounds since convergence = {} ", (curRound - lastConvergenceRound));
                convergenceTs = -1;
            }

            // TODO: execute new behaviors: add/remove node (and replace barrier in simulators) and other actions
            // bounce nodes
        }

        String hasConvergedByInspection()
        {
            List<Future<String>> futures = new ArrayList<>();
            for (Map.Entry<InetAddress, Gossiper> entry : customMessagingService.gossipers.entrySet())
            {
                futures.add(executor.submit(new ConvergenceChecker(entry.getValue(), customMessagingService.gossipers)));
            }

            String ret = null;
            try
            {
                // rip through the list of futures looking for any failures. if we do find a failure, don't bother running any other tasks
                for (Future<String> future : futures)
                {
                    try
                    {
                        if (ret == null)
                        {
                            ret = future.get(2, TimeUnit.MINUTES);
                        } else
                        {
                            future.cancel(true);
                        }
                    }
                    catch (Exception e)
                    {
                        //ignore
                    }
                }
            }
            catch(Exception e)
            {
                logger.warn("Problem while checking for convergence", e);
            }
            return ret;
        }

        public boolean hasConverged()
        {
            return convergedByInspection == null;
        }
    }

    protected class ConvergenceChecker implements Callable<String>
    {
        private final InetAddress localAddr;
        private final Gossiper gossiper;
        private final Map<InetAddress, Gossiper> gossipers;

        ConvergenceChecker(Gossiper gossiper, Map<InetAddress, Gossiper> gossipers)
        {
            this.gossiper = gossiper;
            this.localAddr = gossiper.broadcastAddr;
            this.gossipers = gossipers;
        }

        /*
          Yes, this is lame to simply use the nullness of a string to determine if we've actually converged, but I'm feeling lazy today ....
         */
        public String call() throws Exception
        {
            Collection<InetAddress> peerAddrs = new ArrayList<>();
            peerAddrs.addAll(gossipers.keySet());

            for (Map.Entry<InetAddress, EndpointState> peer : gossiper.endpointStateMap.entrySet())
            {
                InetAddress peerAddr = peer.getKey();
                //this case *really* shouldn't fail - would seem to be more of my error than anything else
                if (!peerAddrs.remove(peerAddr))
                    return String.format("hasConvergedByInspection: node %s didn't find itself in the list of gossipers (peers) - huh?", localAddr);
                if (peerAddr.equals(localAddr))
                    continue;

                // simulator knows about peer, now let's compare states
                EndpointState localEndpointState = peer.getValue();
                if (!localEndpointState.isAlive())
                {
                    return String.format("hasConvergedByInspection: node %s has marked peer %s as 'dead' (not alive)", localAddr, peerAddr);
                }
                EndpointState peerEndpointState = gossipers.get(peerAddr).getEndpointStateForEndpoint(peerAddr);

                // first compare the heartbeats
                //NOTE: the heartBeat.version is almost guaranteed to be different (non-convergent), especially in anything larger than a very small cluster,
                // as the target/source node updates it's heartbeat.version on every gossip round. thus, don't bother to compare them
                if (localEndpointState.getHeartBeatState().getGeneration() != peerEndpointState.getHeartBeatState().getGeneration())
                {
                    return String.format("hasConvergedByInspection: generations are different: local ({}) = %d, target ({}) = %d",
                            localEndpointState.getHeartBeatState().getGeneration(), peerEndpointState.getHeartBeatState().getGeneration());
                }

                // next, compare the app states
                Collection<ApplicationState> peerAppStates = new ArrayList<>();
                peerAppStates.addAll(peerEndpointState.applicationState.keySet());
                for (Map.Entry<ApplicationState, VersionedValue> localAppStateEntry : localEndpointState.applicationState.entrySet())
                {
                    ApplicationState appState = localAppStateEntry.getKey();
                    if (!peerAppStates.remove(appState))
                    {
                        return String.format("hasConvergedByInspection: unknown app state: peer %s does not have AppState %s that local does",
                                peerAddr, appState, localAddr);
                    }
                    if (localAppStateEntry.getValue().compareTo(peerEndpointState.getApplicationState(appState)) != 0)
                    {
                        return String.format("hasConvergedByInspection: divergent app state: AppState %s has local(%s) version %s and peer(%s) version %s",
                                appState, localAddr, localAppStateEntry.getValue().value, peerAddr, peerEndpointState.getApplicationState(appState).value);
                    }
                }
                if (!peerAppStates.isEmpty())
                {
                    return String.format("hasConvergedByInspection: unknown app states: current node %s doesn't know about the following app states from %s: %s", localAddr, peerAddr, peerAppStates);
                }
            }

            if (!peerAddrs.isEmpty())
            {
                if (peerAddrs.size() == gossipers.size() - 1)
                    return String.format("hasConvergedByInspection: unknown nodes: current node %s only knows about itself", localAddr);
                else
                    return String.format("hasConvergedByInspection: unknown nodes: current node %s doesn't know about %d/%d nodes",
                            localAddr, peerAddrs.size(), gossipers.size());
            }
            return null;
        }
    }
}
