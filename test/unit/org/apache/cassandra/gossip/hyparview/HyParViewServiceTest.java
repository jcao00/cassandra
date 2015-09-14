package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.locator.SeedProvider;

import static org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority.HIGH;
import static org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority.LOW;

public class HyParViewServiceTest
{
    static final String LOCAL_DC = "dc0";
    static final String REMOTE_DC_1 = "dc1";
    static final String REMOTE_DC_2 = "dc2`";

    static InetAddress localNodeAddr;
    ExecutorService executorService;
    ScheduledExecutorService scheduler;

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
    }

    @After
    public void tearDown()
    {
        executorService.shutdownNow();
        scheduler.shutdownNow();
    }

    HyParViewService buildService()
    {
        return buildService(Collections.emptyList());
    }

    HyParViewService buildService(List<InetAddress> seeds)
    {
        SeedProvider seedProvider = new TestSeedProvider(seeds);
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);

        hpvService.testInit(new TestMessageSender(), executorService, scheduler);

        return hpvService;
    }

    @Test
    public void addToLocalActiveView_AddSelf()
    {
        HyParViewService hpvService = buildService();
        hpvService.addToView(localNodeAddr, LOCAL_DC);
        Assert.assertEquals(0, hpvService.getPeers().size());
    }

    @Test
    public void addToLocalActiveView_AddOther() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void addToRemoteActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addToView(peer2, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(peer2));
    }

    @Test
    public void addToRemoteActiveView_MultipleDCs() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addToView(peer2, REMOTE_DC_2);
        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(peer2));
    }

    /*
        tests for starting the join of the PeerSamplingService
     */

    @Test
    public void join_NullSeeds()
    {
        HyParViewService hpvService = buildService(null);
        hpvService.join();
        Assert.assertTrue(((TestMessageSender)hpvService.messageSender).messages.isEmpty());
    }

    @Test
    public void join_EmptySeeds()
    {
        HyParViewService hpvService = buildService(Collections.emptyList());
        hpvService.join();
        Assert.assertTrue(((TestMessageSender)hpvService.messageSender).messages.isEmpty());
    }

    @Test
    public void join_SeedIsSelf()
    {
        HyParViewService hpvService = buildService(Collections.singletonList(localNodeAddr));
        hpvService.join();
        Assert.assertTrue(((TestMessageSender) hpvService.messageSender).messages.isEmpty());
    }

    @Test
    public void join() throws UnknownHostException
    {
        InetAddress seed = InetAddress.getByName("127.0.0.42");
        HyParViewService hpvService = buildService(Collections.singletonList(seed));
        hpvService.join();
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());
        SentMessage msg = sender.messages.remove(0);
        Assert.assertEquals(seed, msg.destination);
    }

    /*
        tests for handling the join request
        checks for: peer in active view  --- send join response ---  check for forward join msgs
     */

    @Test
    public void handleJoin_NoForwarding() throws UnknownHostException
    {
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        HyParViewService hpvService = buildService();
        hpvService.handleJoin(new JoinMessage(peer, LOCAL_DC));

        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());
        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(peer, msg.destination);
        Assert.assertEquals(HPVMessageType.JOIN_RESPONSE, msg.message.getMessageType());
    }

    @Test
    public void handleJoin_WithForwarding() throws UnknownHostException
    {
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        InetAddress existingPeer = InetAddress.getByName("127.0.0.3");
        HyParViewService hpvService = buildService();
        hpvService.addToView(existingPeer, LOCAL_DC);
        hpvService.handleJoin(new JoinMessage(peer, LOCAL_DC));

        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(existingPeer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(2, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(peer, msg.destination);
        Assert.assertEquals(HPVMessageType.JOIN_RESPONSE, msg.message.getMessageType());

        msg = sender.messages.get(1);
        Assert.assertEquals(existingPeer, msg.destination);
        Assert.assertEquals(HPVMessageType.FORWARD_JOIN, msg.message.getMessageType());
    }

    @Test
    public void handleForwardJoin_SameDC_Accept() throws UnknownHostException
    {
        handleForwardJoin_Accept(LOCAL_DC);
    }

    @Test
    public void handleForwardJoin_DifferentDC_Accept() throws UnknownHostException
    {
        handleForwardJoin_Accept(REMOTE_DC_1);
    }

    void handleForwardJoin_Accept(String datacenter) throws UnknownHostException
    {
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        InetAddress forwarder = InetAddress.getByName("127.0.0.3");
        HyParViewService hpvService = buildService();
        hpvService.addToView(forwarder, LOCAL_DC);

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleForwardJoin(new ForwardJoinMessage(peer, datacenter, forwarder, 1));

        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(peer, msg.destination);
        Assert.assertEquals(HPVMessageType.JOIN_RESPONSE, msg.message.getMessageType());
    }

    @Test
    public void handleForwardJoin_SameDC_Forward() throws UnknownHostException
    {
        handleForwardJoin_Forward(LOCAL_DC);
    }

    @Test
    public void handleForwardJoin_DifferentDC_Forward() throws UnknownHostException
    {
        handleForwardJoin_Forward(REMOTE_DC_1);
    }

    void handleForwardJoin_Forward(String datacenter) throws UnknownHostException
    {
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        InetAddress forwarder = InetAddress.getByName("127.0.0.3");
        HyParViewService hpvService = buildService();
        hpvService.addToView(forwarder, datacenter);
        hpvService.addToView(InetAddress.getByName("127.0.0.4"), LOCAL_DC);
        hpvService.addToView(InetAddress.getByName("127.0.0.5"), LOCAL_DC);

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleForwardJoin(new ForwardJoinMessage(peer, datacenter, forwarder, 2));

        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.FORWARD_JOIN, msg.message.getMessageType());
    }

    @Test
    public void findArbitraryTarget_LocalDC_EmptyPeers() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        Optional<InetAddress> target = hpvService.findArbitraryTarget(InetAddress.getByName("127.0.0.2"), LOCAL_DC);
        Assert.assertFalse(target.isPresent());
    }

    @Test
    public void findArbitraryTarget_LocalDC_Filtered() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer1 = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer1, LOCAL_DC);
        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, LOCAL_DC);
        Assert.assertFalse(target.isPresent());
    }

    @Test
    public void findArbitraryTarget_LocalDC_Simple() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer1 = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer1, LOCAL_DC);
        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addToView(peer2, LOCAL_DC);
        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, LOCAL_DC);
        Assert.assertTrue(target.isPresent());
        Assert.assertEquals(peer2, target.get());
    }

    @Test
    public void findArbitraryTarget_LocalDC_Many() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer1 = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer1, LOCAL_DC);

        for (int i = 0; i < 16; i++)
            hpvService.addToView(InetAddress.getByName("127.0.1." + i), LOCAL_DC);

        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, LOCAL_DC);
        Assert.assertTrue(target.isPresent());
        Assert.assertFalse(peer1.equals(target.get()));
    }

    @Test
    public void findArbitraryTarget_RemoteDC_EmptyPeers() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        Optional<InetAddress> target = hpvService.findArbitraryTarget(InetAddress.getByName("127.0.101.2"), REMOTE_DC_1);
        Assert.assertFalse(target.isPresent());
    }

    @Test
    public void findArbitraryTarget_RemoteDC_Filtered() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer1 = InetAddress.getByName("127.0.101.2");
        hpvService.addToView(peer1, REMOTE_DC_1);
        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, REMOTE_DC_1);
        Assert.assertFalse(target.isPresent());
    }

    @Test
    public void findArbitraryTarget_RemoteDC_SelectFromLocalDC() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer1 = InetAddress.getByName("127.0.101.2");
        hpvService.addToView(peer1, REMOTE_DC_1);
        InetAddress peer2 = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer2, LOCAL_DC);
        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, REMOTE_DC_1);
        Assert.assertTrue(target.isPresent());
        Assert.assertEquals(peer2, target.get());
    }

    @Test
    public void findArbitraryTarget_RemoteDC_SelectFromRemoteDC() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer2 = InetAddress.getByName("127.0.101.3");
        hpvService.addToView(peer2, REMOTE_DC_1);
        InetAddress peer3 = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer3, LOCAL_DC);

        InetAddress peer1 = InetAddress.getByName("127.0.101.2");
        Optional<InetAddress> target = hpvService.findArbitraryTarget(peer1, REMOTE_DC_1);
        Assert.assertTrue(target.isPresent());
        Assert.assertEquals(peer2, target.get());
    }

    @Test
    public void handleNeighborRequest_AlreadyInView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.101.3");
        hpvService.addToView(peer, LOCAL_DC);
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, LOCAL_DC, LOW, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.ACCEPT, responseMessage.result);
    }

    @Test
    public void handleNeighborRequest_PriorityHigh() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.101.3");

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, LOCAL_DC, HIGH, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.ACCEPT, responseMessage.result);
    }

    @Test
    public void handleNeighborRequest_PriorityLow_RemoteDC_Accept() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.101.3");

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, REMOTE_DC_1, NeighborRequestMessage.Priority.LOW, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.ACCEPT, responseMessage.result);
    }

    @Test
    public void handleNeighborRequest_PriorityLow_RemoteDC_Deny() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress existingRemotePeer = InetAddress.getByName("127.0.41.3");
        hpvService.addToView(existingRemotePeer, REMOTE_DC_1);

        InetAddress peer = InetAddress.getByName("127.0.101.3");
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, REMOTE_DC_1, NeighborRequestMessage.Priority.LOW, 0));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.DENY, responseMessage.result);
    }

    @Test
    public void handleNeighborRequest_PriorityLow_LocalDC_Accept() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.101.3");

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, LOCAL_DC, NeighborRequestMessage.Priority.LOW, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.ACCEPT, responseMessage.result);
    }

    @Test
    public void handleNeighborRequest_PriorityLow_LocalDC_Deny() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress existingPeer = InetAddress.getByName("127.0.41.3");
        hpvService.addToView(existingPeer, LOCAL_DC);

        InetAddress peer = InetAddress.getByName("127.0.101.3");
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(peer, LOCAL_DC, NeighborRequestMessage.Priority.LOW, 0));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_RESPONSE, msg.message.getMessageType());
        Assert.assertEquals(peer, msg.destination);

        NeighborResponseMessage responseMessage = (NeighborResponseMessage)msg.message;
        Assert.assertEquals(NeighborResponseMessage.Result.DENY, responseMessage.result);
    }

    @Test
    public void handleNeighborResponse_AlreadyInView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.41.3");
        hpvService.addToView(peer, LOCAL_DC);

        Assert.assertTrue(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(peer, LOCAL_DC, NeighborResponseMessage.Result.ACCEPT, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void handleNeighborResponse_Accepted() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.41.3");

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(peer, LOCAL_DC, NeighborResponseMessage.Result.ACCEPT, 0));
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void handleNeighborResponse_Denied_SendAnotherRequest() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress existingPeer = InetAddress.getByName("127.0.41.3");
        hpvService.endpointStateSubscriber.add(existingPeer, LOCAL_DC);

        InetAddress peer = InetAddress.getByName("127.0.0.3");
        int requestCount = 0;
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(peer, LOCAL_DC, NeighborResponseMessage.Result.DENY, requestCount));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage msg = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_REQUEST, msg.message.getMessageType());

        NeighborRequestMessage requestMessage = (NeighborRequestMessage)msg.message;
        Assert.assertEquals(requestCount + 1, requestMessage.neighborRequestsCount);
    }

    @Test
    public void handleNeighborResponse_Denied_RequestCountExceeded() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();

        InetAddress peer = InetAddress.getByName("127.0.0.3");
        int requestCount = HyParViewService.MAX_NEIGHBOR_REQUEST_ATTEMPTS - 1;
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(peer, LOCAL_DC, NeighborResponseMessage.Result.DENY, requestCount));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void getPassivePeer_LocalDc_EmptyPeers() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), LOCAL_DC).isPresent());
    }

    @Test
    public void getPassivePeer_LocalDc_AllPeersInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.addToView(peer, LOCAL_DC);
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), LOCAL_DC).isPresent());
    }

    @Test
    public void getPassivePeer_LocalDc_SomePeersInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.addToView(peer, LOCAL_DC);

        peer = InetAddress.getByName("127.0.0.4");
        hpvService.endpointStateSubscriber.add(peer, LOCAL_DC);

        Optional<InetAddress> passivePeer = hpvService.getPassivePeer(Optional.<InetAddress>empty(), LOCAL_DC);
        Assert.assertTrue(passivePeer.isPresent());
        Assert.assertEquals(peer, passivePeer.get());
    }

    @Test
    public void getPassivePeer_RemoteDc_EmptyPeers()
    {
        HyParViewService hpvService = buildService();
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), REMOTE_DC_1).isPresent());
    }

    @Test
    public void getPassivePeer_RemoteDc_AllPeersInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), REMOTE_DC_1).isPresent());
    }

    @Test
    public void getPassivePeer_RemoteDc_OnePeerInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, REMOTE_DC_1);

        for (int i = 0; i < 8; i++)
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);

        Assert.assertTrue(hpvService.getPassivePeer(Optional.<InetAddress>empty(), REMOTE_DC_1).isPresent());
    }

    @Test
    public void determineNeighborPriority_EmptyPeers()
    {
        HyParViewService hpvService = buildService();
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority(LOCAL_DC));
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority(REMOTE_DC_1));
    }

    @Test
    public void determineNeighborPriority_EmptyLocalDcActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority(LOCAL_DC));
    }

    @Test
    public void determineNeighborPriority() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);
        Assert.assertEquals(LOW, hpvService.determineNeighborPriority(LOCAL_DC));
    }

    @Test
    public void sendNeighborRequest_FilteredOut() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);

        hpvService.sendNeighborRequest(Optional.of(peer), LOCAL_DC);
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void sendNeighborRequest_NotFiltered() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, LOCAL_DC);
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.endpointStateSubscriber.add(peer, LOCAL_DC);

        hpvService.sendNeighborRequest(Optional.of(InetAddress.getByName("127.0.0.4")), LOCAL_DC);
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage sentMessage = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_REQUEST, sentMessage.message.getMessageType());
        Assert.assertEquals(peer, sentMessage.destination);
    }

    @Test
    public void handleDisconnect_NotInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();

        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.handleDisconnect(new DisconnectMessage(peer, LOCAL_DC));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void handleDisconnect_InLocalActiveView() throws UnknownHostException
    {
        handleDisconnect_InActiveView(LOCAL_DC);
    }

    @Test
    public void handleDisconnect_InRemoteActiveView() throws UnknownHostException
    {
        handleDisconnect_InActiveView(REMOTE_DC_1);
    }

    private void handleDisconnect_InActiveView(String datacenter) throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToView(peer, datacenter);
        InetAddress otherPeer = InetAddress.getByName("127.0.0.3");
        hpvService.endpointStateSubscriber.add(otherPeer, datacenter);

        Assert.assertTrue(hpvService.getPeers().contains(peer));
        hpvService.handleDisconnect(new DisconnectMessage(peer, datacenter));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());

        SentMessage sentMessage = sender.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_REQUEST, sentMessage.message.getMessageType());
    }

    @Test
    public void checkConnectivity_EmptyPeers()
    {
        HyParViewService hpvService = buildService();
        hpvService.checkConnectivity();
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void checkConnectivity_AllDcsGood() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        peer = InetAddress.getByName("127.0.2.2");
        hpvService.addToView(peer, REMOTE_DC_2);

        for (int i = 0; i < 8; i++)
        {
            hpvService.addToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
        }

        hpvService.checkConnectivity();
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(0, sender.messages.size());
    }

    @Test
    public void checkConnectivity_NeedsRemoteDc() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addToView(peer, REMOTE_DC_1);

        for (int i = 0; i < 8; i++)
        {
            hpvService.addToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
        }

        hpvService.checkConnectivity();
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(1, sender.messages.size());
    }

    @Test
    public void checkConnectivity_NeedsLocalDc() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addToView(peer, REMOTE_DC_1);
        peer = InetAddress.getByName("127.0.2.2");
        hpvService.addToView(peer, REMOTE_DC_2);

        for (int i = 0; i < 4; i++)
        {
            hpvService.addToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.42." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
        }

        hpvService.checkConnectivity();
        TestMessageSender sender = (TestMessageSender)hpvService.messageSender;
        Assert.assertEquals(4, sender.messages.size());
    }

    /*
        Utility classes
     */

    static class TestSeedProvider implements SeedProvider
    {
        final List<InetAddress> seeds;

        TestSeedProvider(List<InetAddress> seeds)
        {
            this.seeds = seeds;
        }

        public List<InetAddress> getSeeds()
        {
            return seeds;
        }
    }

    static class TestMessageSender implements MessageSender
    {
        List<SentMessage> messages = new LinkedList<>();

        public void send(InetAddress sourceAddr, InetAddress destinationAddr, HyParViewMessage message)
        {
            messages.add(new SentMessage(sourceAddr, destinationAddr, message));
        }
    }

    static class SentMessage
    {
        final InetAddress source;
        final InetAddress destination;
        final HyParViewMessage message;

        SentMessage(InetAddress source, InetAddress destination, HyParViewMessage message)
        {
            this.source = source;
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
