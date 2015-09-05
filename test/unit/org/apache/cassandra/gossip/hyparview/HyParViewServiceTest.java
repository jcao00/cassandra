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
package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.hyparview.HyParViewService.Disconnects;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority.HIGH;
import static org.apache.cassandra.gossip.hyparview.NeighborRequestMessage.Priority.LOW;

public class HyParViewServiceTest
{
    private static final int SEED = 231234237;
    private static final String LOCAL_DC = "dc0";
    private static final String REMOTE_DC_1 = "dc1";
    private static final String REMOTE_DC_2 = "dc2";

    private static InetAddress localNodeAddr;
    private static final GossipMessageId.IdGenerator idGenerator = new GossipMessageId.IdGenerator(42);
    private ExecutorService executorService;
    private ScheduledExecutorService scheduler;
    private Random random;
    private TestMessageSink messageSink;

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
        messageSink = new TestMessageSink();
        MessagingService.instance().addMessageSink(messageSink);
    }

    @After
    public void removeMessageSink()
    {
        MessagingService.instance().clearMessageSinks();
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
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, executorService, scheduler, 2);
        hpvService.testInit(42, -1, -1);

        return hpvService;
    }

    private GossipMessageId generateMessageId()
    {
        return new GossipMessageId.IdGenerator(random.nextInt()).generate();
    }

    @Test
    public void updatePeersInfo_Self() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = hpvService.getLocalAddress();
        Assert.assertTrue(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));

        GossipMessageId msgId = idGenerator.generate();
        HyParViewMessage msg = new JoinResponseMessage(msgId, peer, LOCAL_DC, null);

        hpvService.updatePeersInfo(msg);

        Assert.assertNull(hpvService.getHighestSeenMessageId(peer));
        Assert.assertTrue(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));
    }

    @Test
    public void updatePeersInfo_Simple() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        Assert.assertFalse(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));

        GossipMessageId msgId = idGenerator.generate();
        HyParViewMessage msg = new JoinResponseMessage(msgId, peer, LOCAL_DC, null);

        hpvService.updatePeersInfo(msg);

        Assert.assertEquals(msgId, hpvService.getHighestSeenMessageId(peer));
        Assert.assertTrue(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));
    }

    @Test
    public void updatePeersInfo_ForwardJoin() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        InetAddress originator = InetAddress.getByName("127.0.0.3");
        Assert.assertFalse(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));
        Assert.assertFalse(hpvService.endpointStateSubscriber.getPeers().containsEntry(REMOTE_DC_1, originator));

        GossipMessageId msgId = idGenerator.generate();
        GossipMessageId originatorMsgId = generateMessageId();
        HyParViewMessage msg = new ForwardJoinMessage(msgId, peer, LOCAL_DC, originator, REMOTE_DC_1, 2, originatorMsgId);

        hpvService.updatePeersInfo(msg);

        Assert.assertEquals(msgId, hpvService.getHighestSeenMessageId(peer));
        Assert.assertTrue(hpvService.endpointStateSubscriber.getPeers().containsEntry(LOCAL_DC, peer));
        Assert.assertEquals(originatorMsgId, hpvService.getHighestSeenMessageId(originator));
        Assert.assertTrue(hpvService.endpointStateSubscriber.getPeers().containsEntry(REMOTE_DC_1, originator));
    }

    @Test
    public void hasSeenDisconnect_NoPreviousDisconnects() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        HyParViewMessage msg = new JoinResponseMessage(idGenerator.generate(), InetAddress.getByName("127.0.0.2"), LOCAL_DC, null);
        Assert.assertTrue(hpvService.hasSeenDisconnect(msg, Collections.emptyMap()));
    }

    @Test
    public void hasSeenDisconnect_MoreRecentDisconnectFromPeer() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        GossipMessageId.IdGenerator peerIdGenerator = new GossipMessageId.IdGenerator(2981215);

        HyParViewMessage msg = new JoinResponseMessage(peerIdGenerator.generate(), peer, LOCAL_DC, null);
        Map<InetAddress, Disconnects> disconnects = new HashMap<>();
        disconnects.put(peer, new Disconnects(peerIdGenerator.generate(), null));

        Assert.assertFalse(hpvService.hasSeenDisconnect(msg, disconnects));
    }

    @Test
    public void hasSeenDisconnect_OlderDisconnectFromPeer() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        GossipMessageId.IdGenerator peerIdGenerator = new GossipMessageId.IdGenerator(2981215);

        Map<InetAddress, Disconnects> disconnects = new HashMap<>();
        disconnects.put(peer, new Disconnects(peerIdGenerator.generate(), null));
        HyParViewMessage msg = new JoinResponseMessage(peerIdGenerator.generate(), peer, LOCAL_DC, Optional.<GossipMessageId>empty());

        Assert.assertTrue(hpvService.hasSeenDisconnect(msg, disconnects));
    }

    @Test
    public void hasSeenDisconnect_PeerHasNoDisconnects_SameEpoch() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        GossipMessageId.IdGenerator peerIdGenerator = new GossipMessageId.IdGenerator(2981215);

        Map<InetAddress, Disconnects> disconnects = new HashMap<>();
        disconnects.put(peer, new Disconnects(null, Pair.create(idGenerator.generate(), peerIdGenerator.generate())));

        HyParViewMessage msg = new JoinResponseMessage(peerIdGenerator.generate(), peer, LOCAL_DC, Optional.<GossipMessageId>empty());
        Assert.assertFalse(hpvService.hasSeenDisconnect(msg, disconnects));
    }

    @Test
    public void hasSeenDisconnect_PeerHasNoDisconnects_NextEpoch() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        int epoch = 2981215;
        GossipMessageId.IdGenerator peerIdGenerator = new GossipMessageId.IdGenerator(epoch);

        Map<InetAddress, Disconnects> disconnects = new HashMap<>();
        disconnects.put(peer, new Disconnects(null, Pair.create(idGenerator.generate(), peerIdGenerator.generate())));

        HyParViewMessage msg = new JoinResponseMessage(new GossipMessageId.IdGenerator(epoch + 1).generate(), peer, LOCAL_DC, Optional.<GossipMessageId>empty());
        Assert.assertTrue(hpvService.hasSeenDisconnect(msg, disconnects));
    }

    @Test
    public void hasSeenDisconnect_PeerHasSameDisconnect() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        GossipMessageId.IdGenerator peerIdGenerator = new GossipMessageId.IdGenerator(2981215);

        Map<InetAddress, Disconnects> disconnects = new HashMap<>();
        GossipMessageId disconnectMessageId = idGenerator.generate();
        disconnects.put(peer, new Disconnects(null, Pair.create(disconnectMessageId, peerIdGenerator.generate())));

        HyParViewMessage msg = new JoinResponseMessage(peerIdGenerator.generate(), peer, LOCAL_DC, Optional.of(disconnectMessageId));
        Assert.assertTrue(hpvService.hasSeenDisconnect(msg, disconnects));
    }

    @Test
    public void addToLocalActiveView_AddSelf()
    {
        HyParViewService hpvService = buildService();
        hpvService.addPeerToView(localNodeAddr, LOCAL_DC, idGenerator.generate());
        Assert.assertEquals(0, hpvService.getPeers().size());
    }

    @Test
    public void addToLocalActiveView_AddOther() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void addToRemoteActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addPeerToView(peer2, REMOTE_DC_1, generateMessageId());
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(peer2));
    }

    @Test
    public void addToRemoteActiveView_MultipleDCs() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addPeerToView(peer2, REMOTE_DC_2, generateMessageId());
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
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void join_EmptySeeds()
    {
        HyParViewService hpvService = buildService(Collections.emptyList());
        hpvService.join();
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void join_SeedIsSelf()
    {
        HyParViewService hpvService = buildService(Collections.singletonList(localNodeAddr));
        hpvService.join();
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void join() throws UnknownHostException
    {
        InetAddress seed = InetAddress.getByName("127.0.0.42");
        HyParViewService hpvService = buildService(Collections.singletonList(seed));
        hpvService.join();
        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.remove(0);
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
        hpvService.handleJoin(new JoinMessage(idGenerator.generate(), peer, LOCAL_DC));

        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
        Assert.assertEquals(peer, msg.destination);
        Assert.assertEquals(HPVMessageType.JOIN_RESPONSE, msg.message.getMessageType());
    }

    @Test
    public void handleJoin_WithForwarding() throws UnknownHostException
    {
        InetAddress existingPeer = InetAddress.getByName("127.0.0.3");
        HyParViewService hpvService = buildService();
        hpvService.addPeerToView(existingPeer, LOCAL_DC, generateMessageId());

        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.receiveMessage(new JoinMessage(idGenerator.generate(), peer, LOCAL_DC));

        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(existingPeer));

        Assert.assertEquals(2, messageSink.messages.size());

        SentMessage msg = messageSink.messages.get(0);
        Assert.assertEquals(peer, msg.destination);
        Assert.assertEquals(HPVMessageType.JOIN_RESPONSE, msg.message.getMessageType());

        msg = messageSink.messages.get(1);
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
        hpvService.addPeerToView(forwarder, LOCAL_DC, generateMessageId());

        Assert.assertTrue(hpvService.getPeers().contains(forwarder));
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.receiveMessage(new ForwardJoinMessage(idGenerator.generate(), forwarder, datacenter, peer, LOCAL_DC, 1, null));

        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.addPeerToView(forwarder, datacenter, generateMessageId());
        hpvService.addPeerToView(InetAddress.getByName("127.0.0.4"), LOCAL_DC, generateMessageId());
        hpvService.addPeerToView(InetAddress.getByName("127.0.0.5"), LOCAL_DC, generateMessageId());

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleForwardJoin(new ForwardJoinMessage(idGenerator.generate(), forwarder, datacenter, peer, LOCAL_DC, 2, null));

        Assert.assertFalse(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
        Assert.assertEquals(HPVMessageType.FORWARD_JOIN, msg.message.getMessageType());
    }

    @Test
    public void handleNeighborRequest_AlreadyInView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.101.3");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, LOCAL_DC, LOW, 0, null));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, LOCAL_DC, HIGH, 0, null));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, REMOTE_DC_1, NeighborRequestMessage.Priority.LOW, 0, null));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.addPeerToView(existingRemotePeer, REMOTE_DC_1, generateMessageId());

        InetAddress peer = InetAddress.getByName("127.0.101.3");
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, REMOTE_DC_1, NeighborRequestMessage.Priority.LOW, 0, null));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborRequestMessage.Priority.LOW, 0, null));
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.addPeerToView(existingPeer, LOCAL_DC, generateMessageId());

        InetAddress peer = InetAddress.getByName("127.0.101.3");
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborRequest(new NeighborRequestMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborRequestMessage.Priority.LOW, 0, null));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());

        Assert.assertTrue(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborResponseMessage.Result.ACCEPT, 0, null));
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void handleNeighborResponse_Accepted() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.41.3");

        Assert.assertFalse(hpvService.getPeers().contains(peer));
        hpvService.handleNeighborResponse(new NeighborResponseMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborResponseMessage.Result.ACCEPT, 0, null));
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
        hpvService.handleNeighborResponse(new NeighborResponseMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborResponseMessage.Result.DENY, requestCount, null));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage msg = messageSink.messages.get(0);
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
        hpvService.handleNeighborResponse(new NeighborResponseMessage(idGenerator.generate(), peer, LOCAL_DC, NeighborResponseMessage.Result.DENY, requestCount, null));
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        Assert.assertTrue(messageSink.messages.isEmpty());
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
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), LOCAL_DC).isPresent());
    }

    @Test
    public void getPassivePeer_LocalDc_SomePeersInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());

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
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        Assert.assertFalse(hpvService.getPassivePeer(Optional.<InetAddress>empty(), REMOTE_DC_1).isPresent());
    }

    @Test
    public void getPassivePeer_RemoteDc_OnePeerInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());

        for (int i = 0; i < 8; i++)
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);

        Assert.assertTrue(hpvService.getPassivePeer(Optional.<InetAddress>empty(), REMOTE_DC_1).isPresent());
    }

    @Test
    public void determineNeighborPriority_EmptyPeers()
    {
        HyParViewService hpvService = buildService();
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority());
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority());
    }

    @Test
    public void determineNeighborPriority_EmptyLocalDcActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        Assert.assertEquals(HIGH, hpvService.determineNeighborPriority( ));
    }

    @Test
    public void determineNeighborPriority() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        Assert.assertEquals(LOW, hpvService.determineNeighborPriority());
    }

    @Test
    public void sendNeighborRequest_FilteredOut() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());

        hpvService.sendNeighborRequest(Optional.of(peer), LOCAL_DC);
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void sendNeighborRequest_NotFiltered() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addPeerToView(peer, LOCAL_DC, generateMessageId());
        peer = InetAddress.getByName("127.0.0.3");
        hpvService.endpointStateSubscriber.add(peer, LOCAL_DC);

        hpvService.sendNeighborRequest(Optional.of(InetAddress.getByName("127.0.0.4")), LOCAL_DC);
        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage sentMessage = messageSink.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_REQUEST, sentMessage.message.getMessageType());
        Assert.assertEquals(peer, sentMessage.destination);
    }

    @Test
    public void handleDisconnect_NotInActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();

        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.handleDisconnect(new DisconnectMessage(idGenerator.generate(), peer, LOCAL_DC));

        Assert.assertTrue(messageSink.messages.isEmpty());
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
        hpvService.addPeerToView(peer, datacenter, generateMessageId());
        hpvService.addPeerToView(InetAddress.getByName("127.0.0.4"), LOCAL_DC, generateMessageId());

        hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.0.3"), datacenter);

        Assert.assertTrue(hpvService.getPeers().contains(peer));
        hpvService.handleDisconnect(new DisconnectMessage(idGenerator.generate(), peer, datacenter));
        Assert.assertFalse(hpvService.getPeers().contains(peer));

        Assert.assertEquals(1, messageSink.messages.size());
        SentMessage sentMessage = messageSink.messages.get(0);
        Assert.assertEquals(HPVMessageType.NEIGHBOR_REQUEST, sentMessage.message.getMessageType());
    }

    @Test
    public void checkFullActiveView_EmptyPeers()
    {
        HyParViewService hpvService = buildService();
        hpvService.setHasJoined(true);
        hpvService.checkFullActiveView();
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void checkFullActiveView_AllDcsGood() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        hpvService.setHasJoined(true);
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        peer = InetAddress.getByName("127.0.2.2");
        hpvService.addPeerToView(peer, REMOTE_DC_2, generateMessageId());

        for (int i = 0; i < 8; i++)
        {
            if (i < 4)
                hpvService.addPeerToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC, generateMessageId());
            else
                hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
        }

        hpvService.checkFullActiveView();
        Assert.assertTrue(messageSink.messages.isEmpty());
    }

    @Test
    public void checkFullActiveView_NeedsRemoteDc() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        hpvService.setHasJoined(true);
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());

        for (int i = 0; i < 8; i++)
        {
            if (i < 4)
                hpvService.addPeerToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC, generateMessageId());
            else
                hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + (i * 10)), REMOTE_DC_2);
        }

        hpvService.checkFullActiveView();
        Assert.assertEquals(1, messageSink.messages.size());
    }

    @Test
    public void checkFullActiveView_NeedsLocalDc() throws UnknownHostException
    {
        HyParViewService hpvService = buildService();
        hpvService.setHasJoined(true);
        InetAddress peer = InetAddress.getByName("127.0.1.2");
        hpvService.addPeerToView(peer, REMOTE_DC_1, generateMessageId());
        peer = InetAddress.getByName("127.0.2.2");
        hpvService.addPeerToView(peer, REMOTE_DC_2, generateMessageId());

        for (int i = 0; i < 8; i++)
        {
            if (i < 3)
                hpvService.addPeerToView(InetAddress.getByName("127.0.3." + i), LOCAL_DC, generateMessageId());
            else
                hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.3." + i), LOCAL_DC);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
            hpvService.endpointStateSubscriber.add(InetAddress.getByName("127.0.2." + i), REMOTE_DC_2);
        }

        hpvService.checkFullActiveView();
        Assert.assertEquals(1, messageSink.messages.size());
    }

    @Test
    public void EndpointStateSubscriber_comapreDatacenterSizes_EmptyDcs() throws UnknownHostException
    {
        HyParViewService.EndpointStateSubscriber subscriber = buildService().endpointStateSubscriber;
        setupSubscriber(subscriber, 0, 1);
        Assert.assertEquals(0, subscriber.comapreDatacenterSizes(LOCAL_DC, REMOTE_DC_1));
    }

    private void setupSubscriber(HyParViewService.EndpointStateSubscriber subscriber, int localPeerCount, int remotePeerCount) throws UnknownHostException
    {
        for (int i = 0; i < localPeerCount; i++)
            subscriber.add(InetAddress.getByName("127.0.0." + i), LOCAL_DC);
        for (int i = 0; i < remotePeerCount; i++)
            subscriber.add(InetAddress.getByName("127.0.1." + i), REMOTE_DC_1);
    }

    @Test
    public void EndpointStateSubscriber_comapreDatacenterSizes_EmptyLocal() throws UnknownHostException
    {
        HyParViewService.EndpointStateSubscriber subscriber = buildService().endpointStateSubscriber;
        setupSubscriber(subscriber, 0, 2);
        Assert.assertTrue(subscriber.comapreDatacenterSizes(LOCAL_DC, REMOTE_DC_1) < 0);
    }

    @Test
    public void EndpointStateSubscriber_comapreDatacenterSizes_EmptyRemote() throws UnknownHostException
    {
        HyParViewService.EndpointStateSubscriber subscriber = buildService().endpointStateSubscriber;
        setupSubscriber(subscriber, 1, 0);
        Assert.assertTrue(subscriber.comapreDatacenterSizes(LOCAL_DC, REMOTE_DC_1) > 0);
    }

    @Test
    public void EndpointStateSubscriber_comapreDatacenterSizes_MoreInLocal() throws UnknownHostException
    {
        HyParViewService.EndpointStateSubscriber subscriber = buildService().endpointStateSubscriber;
        setupSubscriber(subscriber, 8, 4);
        Assert.assertTrue(subscriber.comapreDatacenterSizes(LOCAL_DC, REMOTE_DC_1) > 0);
    }

    @Test
    public void EndpointStateSubscriber_comapreDatacenterSizes_MoreInRemote() throws UnknownHostException
    {
        HyParViewService.EndpointStateSubscriber subscriber = buildService().endpointStateSubscriber;
        setupSubscriber(subscriber, 8, 16);
        Assert.assertTrue(subscriber.comapreDatacenterSizes(LOCAL_DC, REMOTE_DC_1) < 0);
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

    static class TestMessageSink implements IMessageSink
    {
        List<SentMessage> messages = new LinkedList<>();

        public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
        {
            assert message.payload instanceof HyParViewMessage;
            messages.add(new SentMessage(to, (HyParViewMessage)message.payload));
            return false;
        }

        public boolean allowIncomingMessage(MessageIn message, int id)
        {
            return false;
        }
    }

    static class SentMessage
    {
        final InetAddress destination;
        final HyParViewMessage message;

        SentMessage(InetAddress destination, HyParViewMessage message)
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
