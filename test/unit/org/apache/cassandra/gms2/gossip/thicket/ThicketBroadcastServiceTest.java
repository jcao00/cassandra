package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService.ExpiringMapEntry;
import org.apache.cassandra.gms2.gossip.thicket.messages.MessageType;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketDataMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;
import org.apache.cassandra.utils.ExpiringMap;

public class ThicketBroadcastServiceTest
{
    final String msgId = "msg0";
    final String msg = "hello, thicket!";

    ThicketBroadcastService<ThicketMessage> thicket;
    InetAddress addr;
    InetAddress sender;

    // an arbitrary address to use as a tree root
    InetAddress treeRoot;
    SimpleClient client;
    Map<InetAddress, Integer> loadEstimate;

    @Before
    public void setup() throws UnknownHostException
    {
        treeRoot = InetAddress.getByName("127.10.13.0");
        thicket = new ThicketBroadcastService<>(new ThicketConfigImpl(treeRoot), new AddressRecordingDispatcher());
        addr = InetAddress.getByName("127.0.0.1");
        sender = InetAddress.getByName("127.0.0.2");
        client = new SimpleClient();
        loadEstimate = new HashMap<>();
    }

    @Test
    public void getTargets_AtRootNode_EmptyPeers()
    {
        Assert.assertEquals(0, thicket.getTargets(null, addr).size());
    }

    @Test
    public void getTargets_AtRootNode_SmallPeerSet() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        addrs.add(InetAddress.getByName("127.0.0.3"));
        thicket.setBackupPeers(addrs);
        Collection<InetAddress> peers = thicket.getTargets(null, thicket.getAddress());
        Assert.assertEquals(addrs.size(), peers.size());
        Assert.assertFalse(peers.contains(thicket.getAddress()));
    }

    @Test
    public void getTargets_AtRootNode_LargePeerSet() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 20; i++)
            addrs.add(InetAddress.getByName("127.0.4." + i));
        thicket.setBackupPeers(addrs);
        Collection<InetAddress> peers = thicket.getTargets(null, thicket.getAddress());
        Assert.assertEquals(thicket.getFanout(), peers.size());
        Assert.assertFalse(peers.contains(thicket.getAddress()));

        // now, check that fetching from cache is working properly
        Collection<InetAddress> refetchedPeers = thicket.getTargets(null, thicket.getAddress());
        Assert.assertTrue(peers.containsAll(refetchedPeers));
        Assert.assertTrue(refetchedPeers.containsAll(peers));
    }

    @Test
    public void getTargets_AtRootNode_LargePeerSet_GetFromCache() throws UnknownHostException
    {
        // first prime the pump
        getTargets_AtRootNode_LargePeerSet();

    }

    /*
        'FirstDegree' means this node is an immediate branch of a tree root
     */
    @Test
    public void getTargets_FirstDegreeNode_EmptyPeers() throws UnknownHostException
    {
        Collection<InetAddress> peers = thicket.getTargets(treeRoot, treeRoot);
        Assert.assertEquals(1, peers.size());
        Assert.assertEquals(treeRoot, peers.iterator().next());
    }

    @Test
    public void getTargets_FirstDegreeNode_SmallPeerSet() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        addrs.add(InetAddress.getByName("127.0.0.3"));
        thicket.setBackupPeers(addrs);

        Collection<InetAddress> peers = thicket.getTargets(treeRoot, treeRoot);
        Assert.assertEquals(thicket.getFanout(), peers.size());
        Assert.assertTrue(peers.contains(treeRoot));
    }

    @Test
    public void getTargets_FirstDegreeNode_SmallPeerSetWithTreeRoot() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        addrs.add(treeRoot);
        thicket.setBackupPeers(addrs);
        Collection<InetAddress> peers = thicket.getTargets(treeRoot, treeRoot);
        Assert.assertEquals(addrs.size(), peers.size());
        Assert.assertTrue(peers.toString(), peers.contains(sender));
        Assert.assertTrue(peers.toString(), peers.contains(treeRoot));
    }

    @Test
    public void getTargets_FirstDegreeNode_LargePeerSet() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 20; i++)
            addrs.add(InetAddress.getByName("127.0.4." + i));

        thicket.setBackupPeers(addrs);
        Collection<InetAddress> peers = thicket.getTargets(treeRoot, treeRoot);
        Assert.assertEquals(addrs.size(), peers.size());
        Assert.assertTrue(peers.contains(treeRoot));
    }

    @Test
    public void getTargets_FirstDegreeNode_LargePeerSet_GetFromCache() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 20; i++)
            addrs.add(InetAddress.getByName("127.0.4." + i));

        thicket.setBackupPeers(addrs);
        InetAddress sender = addrs.get(0);
        Collection<InetAddress> peers = thicket.getTargets(treeRoot, treeRoot);
        Assert.assertEquals(addrs.size(), peers.size());
        Assert.assertTrue(peers.contains(treeRoot));

        Collection<InetAddress> refetchedPeers = thicket.getTargets(sender, treeRoot);
        Assert.assertTrue(peers.containsAll(refetchedPeers));
        Assert.assertTrue(refetchedPeers.containsAll(peers));

        InetAddress nextSender = addrs.get(1);
        Collection<InetAddress> peersForSender = thicket.getTargets(nextSender, treeRoot);
        Assert.assertTrue(peersForSender.contains(nextSender));
        Assert.assertTrue(peersForSender.containsAll(refetchedPeers));
    }

    /*
        'SecondDegree' means this node is not an immediate branch of a tree root, meaning that there is at least one intermediary branch
        between the tree node and this node.
     */
    @Test
    public void getTargets_SecondDegreeNode_EmptyPeers() throws UnknownHostException
    {
        Collection<InetAddress> peers = thicket.getTargets(sender, treeRoot);
        Assert.assertEquals(1, peers.size());
        Assert.assertEquals(sender, peers.iterator().next());
    }

    @Test
    public void getTargets_SecondDegreeNode_SmallPeerSet() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        addrs.add(InetAddress.getByName("127.0.0.3"));
        thicket.setBackupPeers(addrs);

        Collection<InetAddress> peers = thicket.getTargets(sender, treeRoot);
        Assert.assertEquals(thicket.getFanout(), peers.size());
        Assert.assertFalse(peers.contains(treeRoot));
    }

    @Test
    public void getTargets_SecondDegreeNode_SmallPeerSetWithTreeRoot() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        addrs.add(treeRoot);
        thicket.setBackupPeers(addrs);

        Collection<InetAddress> peers = thicket.getTargets(sender, treeRoot);
        Assert.assertEquals(peers.toString(), thicket.getFanout() - 1, peers.size());
        Assert.assertFalse(peers.contains(treeRoot));
    }

    @Test
    public void getTargets_InteriorNode() throws UnknownHostException
    {
        // first, use an existing function to get the thicket instance already in one tree
        getTargets_FirstDegreeNode_LargePeerSet();

        // now try to get next tree for a different tree root
        Collection<InetAddress> peers = thicket.getTargets(sender, sender);
        Assert.assertEquals(1, peers.size());
        Assert.assertTrue(peers.contains(sender));
    }

    @Test
    public void broadcast_WithPeers()
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        thicket.setBackupPeers(addrs);

        thicket.broadcast(client.getClientId(), msgId, msg);
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.destinations.toString(), 1, dispatcher.destinations.size());
        Assert.assertTrue(dispatcher.destinations.contains(sender));
        assertHasRecentMessage(client.getClientId(), msgId, treeRoot, treeRoot);
    }

    private void assertHasRecentMessage(String clientId, String msgId, InetAddress treRoot, InetAddress sender)
    {
        ConcurrentMap<String, HashMap<ReceivedMessage, InetAddress>> recentMessages = thicket.getRecentMessages();
        Assert.assertTrue(recentMessages.containsKey(clientId));
        HashMap<ReceivedMessage, InetAddress> clientMessages = recentMessages.get(clientId);
        ReceivedMessage receivedMessage = new ReceivedMessage(msgId, treRoot);
        Assert.assertTrue("client msgs = " + clientMessages.toString() + "\nreceived msg = " + receivedMessage,
                          clientMessages.containsKey(receivedMessage));
        Assert.assertEquals(sender, clientMessages.get(receivedMessage));
    }

    @Test
    public void broadcast_WithNoPeers()
    {
        thicket.broadcast(client.getClientId(), msgId, msg);
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.destinations.toString(), 0, dispatcher.destinations.size());
        Assert.assertTrue(thicket.getRecentMessages().isEmpty());
    }

    @Test
    public void handleData_FreshMessage_NoDownstreamPeers() throws IOException
    {
        thicket.register(client);
        ThicketDataMessage thicketMessage = new ThicketDataMessage(treeRoot, client.getClientId(), msgId, msg, loadEstimate);

        // first, verify the message was delivered and processed
        thicket.handleDataMessage(thicketMessage, treeRoot);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);

        // next, make sure it was not broadcast downstream to peers
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.destinations.toString(), 0, dispatcher.destinations.size());
        assertHasRecentMessage(client.getClientId(), msgId, treeRoot, treeRoot);
    }

    @Test
    public void handleData_FreshMessage_WithDownstreamPeers() throws IOException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        thicket.setBackupPeers(addrs);
        thicket.register(client);
        ThicketDataMessage thicketMessage = new ThicketDataMessage(treeRoot, client.getClientId(), msgId, msg, loadEstimate);

        // first, verify the message was delivered and processed
        thicket.handleDataMessage(thicketMessage, treeRoot);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);

        // next, make sure it was broadcast downstream to peers
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.destinations.toString(), 1, dispatcher.destinations.size());
        Assert.assertTrue(dispatcher.destinations.contains(sender));
        assertHasRecentMessage(client.getClientId(), msgId, treeRoot, treeRoot);
    }

    @Test
    public void handleData_StaleMessage_NoDownstreamPeers() throws IOException
    {
        client.isFreshMessage(false);
        thicket.register(client);
        ThicketDataMessage thicketMessage = new ThicketDataMessage(treeRoot, client.getClientId(), msgId, msg, loadEstimate);

        // first, verify the message was delivered and processed
        thicket.handleDataMessage(thicketMessage, treeRoot);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);

        // next, make sure a prune message was sent back to the sender
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.messages.toString(), 1, dispatcher.messages.size());
        Assert.assertEquals(MessageType.PRUNE, dispatcher.messages.get(0).msg.getMessageType());
        Assert.assertTrue(thicket.getRecentMessages().isEmpty());
    }

    @Test
    public void handleData_StaleMessage_WithDownstreamPeers() throws IOException
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        thicket.setBackupPeers(addrs);

        client.isFreshMessage(false);
        thicket.register(client);
        ThicketDataMessage thicketMessage = new ThicketDataMessage(treeRoot, client.getClientId(), msgId, msg, loadEstimate);

        // first, verify the message was delivered and processed
        thicket.handleDataMessage(thicketMessage, treeRoot);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);

        // next, make sure it was not broadcast downstream to peers
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertFalse(dispatcher.destinations.contains(sender));

        // last, make sure a prune message was sent back to the sender
        Assert.assertEquals(dispatcher.messages.toString(), 1, dispatcher.messages.size());
        Assert.assertEquals(treeRoot, dispatcher.messages.get(0).addr);
        Assert.assertEquals(MessageType.PRUNE, dispatcher.messages.get(0).msg.getMessageType());
        Assert.assertTrue(thicket.getRecentMessages().isEmpty());
    }

    @Test
    public void handleData_RemoveAnnouncement() throws IOException
    {
        thicket.register(client);
        ExpiringMap<ExpiringMapEntry, CopyOnWriteArrayList<InetAddress>> announcements = thicket.getAnnouncements();
        ExpiringMapEntry entry = new ExpiringMapEntry(client.getClientId(), msgId, treeRoot);
        CopyOnWriteArrayList<InetAddress> peers = new CopyOnWriteArrayList<>();
        peers.add(addr);
        announcements.put(entry, peers);
        Assert.assertFalse(announcements.isEmpty());
        ThicketDataMessage thicketMessage = new ThicketDataMessage(treeRoot, client.getClientId(), msgId, msg, loadEstimate);

        thicket.handleDataMessage(thicketMessage, treeRoot);
        Assert.assertTrue(announcements.isEmpty());
    }

    @Test
    public void alreadyInView_EmptyViews()
    {
        Assert.assertFalse(thicket.alreadyInView(treeRoot));
    }

    @Test
    public void alreadyInView_InBackupPeers()
    {
        List<InetAddress> addrs = new ArrayList<>();
        addrs.add(sender);
        thicket.setBackupPeers(addrs);
        Assert.assertFalse(thicket.alreadyInView(treeRoot));
        Assert.assertTrue(thicket.alreadyInView(sender));
    }

    @Test
    public void alreadyInView_NotInBackupPeers() throws UnknownHostException
    {
        List<InetAddress> addrs = new ArrayList<>();
        for (int i = 0; i < 20; i++)
            addrs.add(InetAddress.getByName("127.0.4." + i));

        thicket.setBackupPeers(addrs);
        InetAddress sender = addrs.get(0);
        thicket.getTargets(sender, treeRoot);

        // reset the backup peers so we don't trigger on that
        thicket.setBackupPeers(Collections.<InetAddress>emptyList());

        Assert.assertTrue(thicket.alreadyInView(treeRoot));
        Assert.assertTrue(thicket.alreadyInView(sender));
    }

    @Test
    public void calculateForwardingLoad_Null()
    {
        Assert.assertEquals(-1, thicket.calculateForwardingLoad(null));
    }

    @Test
    public void calculateForwardingLoad_Empty()
    {
        Assert.assertEquals(-1, thicket.calculateForwardingLoad(new HashMap<InetAddress, Integer>()));
    }

    @Test
    public void calculateForwardingLoad_LegitMap() throws UnknownHostException
    {
        Map<InetAddress, Integer> map = new HashMap<>();
        int count = 0;
        for (int i = 0; i < 20; i++)
        {
            map.put(InetAddress.getByName("127.0.4." + i), i);
            count += i;
        }
        Assert.assertEquals(count, thicket.calculateForwardingLoad(map));
    }

    @Test
    public void calculateForwardingLoad_MapWithNulls() throws UnknownHostException
    {
        Map<InetAddress, Integer> map = new HashMap<>();
        int count = 0;
        for (int i = 0; i < 20; i++)
        {
            if (i % 2 == 0)
            {
                map.put(InetAddress.getByName("127.0.4." + i), i);
                count += i;
            }
            else
            {
                map.put(InetAddress.getByName("127.0.4." + i), null);
            }
        }
        Assert.assertEquals(count, thicket.calculateForwardingLoad(map));
    }

    @Test
    public void isInterior_EmptyActivePeers()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        Assert.assertFalse(thicket.isInterior(activePeers));
    }

    @Test
    public void isInterior_NotInterior() throws UnknownHostException
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        for (int i = 0; i < 8; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.4." + i);
            CopyOnWriteArraySet<InetAddress> branches = new CopyOnWriteArraySet<>();
            branches.add(InetAddress.getByName("127.0.42." + i));
            activePeers.put(treeRoot, branches);
        }
        Assert.assertFalse(thicket.isInterior(activePeers));
    }

    @Test
    public void isInterior_Interior() throws UnknownHostException
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        for (int i = 0; i < 8; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.4." + i);
            CopyOnWriteArraySet<InetAddress> branches = new CopyOnWriteArraySet<>();
            branches.add(InetAddress.getByName("127.0.42." + i));
            activePeers.put(treeRoot, branches);
        }

        // select a random tree to add another branch to
        InetAddress addr = Utils.selectRandom(activePeers.keySet());
        CopyOnWriteArraySet<InetAddress> branches = activePeers.get(addr);
        branches.add(InetAddress.getByName("42.42.42.0"));
        branches.add(InetAddress.getByName("42.42.42.3"));
        branches.add(InetAddress.getByName("42.42.42.5"));

        Assert.assertTrue(thicket.isInterior(activePeers));
    }

    @Test
    public void buildLoadEstimate_EmptyPeers()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        Map<InetAddress, Integer> loadEst = thicket.buildLoadEstimate(activePeers);
        Assert.assertTrue(loadEst.isEmpty());
    }

    @Test
    public void buildLoadEstimate_WithPeers() throws UnknownHostException
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        for (int i = 0; i < 4; i++)
        {
            InetAddress treeRoot = InetAddress.getByName("127.0.4." + i);
            CopyOnWriteArraySet<InetAddress> branches = new CopyOnWriteArraySet<>();
            branches.add(InetAddress.getByName("127.0.42." + i));
            activePeers.put(treeRoot, branches);
        }

        CopyOnWriteArraySet<InetAddress> branches = activePeers.get(InetAddress.getByName("127.0.4.0"));
        branches.add(InetAddress.getByName("42.42.42.0"));
        branches.add(InetAddress.getByName("42.42.42.3"));
        branches.add(InetAddress.getByName("42.42.42.5"));

        // should filtered out in result map
        InetAddress emptyAddr = InetAddress.getByName("127.0.7.0");
        activePeers.put(emptyAddr, new CopyOnWriteArraySet<InetAddress>());

        Map<InetAddress, Integer> loadEst = thicket.buildLoadEstimate(activePeers);
        Assert.assertFalse(loadEst.isEmpty());
        Assert.assertEquals(4, loadEst.size());

        Assert.assertEquals(4, loadEst.get(InetAddress.getByName("127.0.4.0")).intValue());
        Assert.assertEquals(1, loadEst.get(InetAddress.getByName("127.0.4.1")).intValue());
        Assert.assertEquals(1, loadEst.get(InetAddress.getByName("127.0.4.2")).intValue());
        Assert.assertEquals(1, loadEst.get(InetAddress.getByName("127.0.4.3")).intValue());

        Assert.assertFalse(loadEst.containsKey(emptyAddr));
    }

    @Test
    public void removeActivePeer_EmptyActivePeers()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        Assert.assertFalse(thicket.removeActivePeer(activePeers, treeRoot, addr));

    }

    @Test
    public void removeActivePeer_NonEmptyActivePeers()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArraySet<InetAddress> branches = new CopyOnWriteArraySet<>();
        branches.add(addr);
        activePeers.put(treeRoot, branches);

        Assert.assertTrue(thicket.removeActivePeer(activePeers, treeRoot, addr));
        Assert.assertTrue(thicket.getBackupPeers().contains(addr));
    }

    @Test
    public void maybeReconfigure_NoPreviousAnnouncement()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<InetAddress> previousSenders = new CopyOnWriteArrayList<>();
        Map<InetAddress, Integer> loadEst = new HashMap<>();
        Assert.assertFalse(thicket.maybeReconfigure(client.getClientId(), treeRoot, sender, activePeers, previousSenders, loadEst));
    }

    @Test
    public void maybeReconfigure_AnnouncementFromSameSender()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<InetAddress> previousSenders = new CopyOnWriteArrayList<>();
        previousSenders.add(sender);
        Map<InetAddress, Integer> loadEst = new HashMap<>();
        Assert.assertFalse(thicket.maybeReconfigure(client.getClientId(), treeRoot, sender, activePeers, previousSenders, loadEst));
    }

    @Test
    public void maybeReconfigure_NoLoadEstimate()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<InetAddress> previousSenders = new CopyOnWriteArrayList<>();
        previousSenders.add(addr);
        Map<InetAddress, Integer> loadEst = new HashMap<>();
        loadEst.put(sender, 2);
        Assert.assertFalse(thicket.maybeReconfigure(client.getClientId(), treeRoot, sender, activePeers, previousSenders, loadEst));
    }

    @Test
    public void maybeReconfigure_SenderLoadEstimateIsHigher()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<InetAddress> previousSenders = new CopyOnWriteArrayList<>();
        previousSenders.add(addr);
        Map<InetAddress, Integer> loadEst = new HashMap<>();
        loadEst.put(sender, 4);
        loadEst.put(addr, 2);
        Assert.assertTrue(thicket.maybeReconfigure(client.getClientId(), treeRoot, sender, activePeers, previousSenders, loadEst));
    }

    @Test
    public void maybeReconfigure_SenderLoadEstimateIsLower()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArrayList<InetAddress> previousSenders = new CopyOnWriteArrayList<>();
        previousSenders.add(addr);
        Map<InetAddress, Integer> loadEst = new HashMap<>();
        loadEst.put(sender, 4);
        loadEst.put(addr, 8);
        Assert.assertFalse(thicket.maybeReconfigure(client.getClientId(), treeRoot, sender, activePeers, previousSenders, loadEst));
    }

    @Test
    public void addToRecentMessages_NewClient()
    {
        ConcurrentMap<String, HashMap<ReceivedMessage, InetAddress>> recentMessages = new ConcurrentHashMap<>();
        thicket.addToRecentMessages(client.getClientId(), msgId, treeRoot, sender, recentMessages);

        Assert.assertTrue(recentMessages.containsKey(client.getClientId()));
        HashMap<ReceivedMessage, InetAddress> map = recentMessages.get(client.getClientId());
        Assert.assertEquals(1, map.size());
        Map.Entry<ReceivedMessage, InetAddress> entry = map.entrySet().iterator().next();
        ReceivedMessage msg = entry.getKey();
        Assert.assertEquals(msg.msgId, msgId);
        Assert.assertEquals(msg.treeRoot, treeRoot);
        Assert.assertEquals(sender, entry.getValue());
    }

    @Test
    public void addToRecentMessages_NewClient_DupeMessage() throws UnknownHostException
    {
        ConcurrentMap<String, HashMap<ReceivedMessage, InetAddress>> recentMessages = new ConcurrentHashMap<>();
        thicket.addToRecentMessages(client.getClientId(), msgId, treeRoot, sender, recentMessages);

        InetAddress newAddr = InetAddress.getByName("127.0.7.0");
        thicket.addToRecentMessages(client.getClientId(), msgId, treeRoot, newAddr, recentMessages);

        Assert.assertTrue(recentMessages.containsKey(client.getClientId()));
        HashMap<ReceivedMessage, InetAddress> map = recentMessages.get(client.getClientId());
        Assert.assertEquals(1, map.size());
        Map.Entry<ReceivedMessage, InetAddress> entry = map.entrySet().iterator().next();
        ReceivedMessage msg = entry.getKey();
        Assert.assertEquals(msg.msgId, msgId);
        Assert.assertEquals(msg.treeRoot, treeRoot);

        // should be the original sender
        Assert.assertEquals(sender, entry.getValue());
    }

    @Test
    public void addToActivePeers_NewTreeRoot()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        thicket.addToActivePeers(activePeers, treeRoot, addr);
        Assert.assertTrue(activePeers.containsKey(treeRoot));
        Assert.assertEquals(1, activePeers.get(treeRoot).size());
        Assert.assertTrue(activePeers.get(treeRoot).contains(addr));
    }

    @Test
    public void addToActivePeers_ExistingTreeRoot()
    {
        ConcurrentMap<InetAddress, CopyOnWriteArraySet<InetAddress>> activePeers = new ConcurrentHashMap<>();
        CopyOnWriteArraySet<InetAddress> branches = new CopyOnWriteArraySet<>();
        branches.add(sender);
        activePeers.put(treeRoot, branches);

        thicket.addToActivePeers(activePeers, treeRoot, addr);
        Assert.assertTrue(activePeers.containsKey(treeRoot));
        Assert.assertEquals(2, activePeers.get(treeRoot).size());
        Assert.assertTrue(activePeers.get(treeRoot).contains(addr));
        Assert.assertTrue(activePeers.get(treeRoot).contains(sender));
    }

    @Test
    public void findGraftAlternate_KnownLoadEst() throws UnknownHostException
    {
        List<InetAddress> peers = new LinkedList<>();

        ConcurrentMap<InetAddress, Map<InetAddress, Integer>> loadEstimates = new ConcurrentHashMap<>();
        InetAddress addr1 = InetAddress.getByName("127.0.0.1");
        Map<InetAddress, Integer> map = new HashMap<>();
        map.put(treeRoot, 3);
        map.put(sender, 1);
        loadEstimates.put(addr1, map);
        peers.add(addr1);

        InetAddress addr2 = InetAddress.getByName("127.0.0.2");
        map = new HashMap<>();
        map.put(treeRoot, 1);
        loadEstimates.put(addr2, map);
        peers.add(addr2);

        Assert.assertEquals(addr2, thicket.findGraftAlternate(peers, loadEstimates));
    }

    @Test
    public void findGraftAlternate_OneUnknownLoadEst() throws UnknownHostException
    {
        List<InetAddress> peers = new LinkedList<>();

        ConcurrentMap<InetAddress, Map<InetAddress, Integer>> loadEstimates = new ConcurrentHashMap<>();
        InetAddress addr1 = InetAddress.getByName("127.0.0.1");
        Map<InetAddress, Integer> map = new HashMap<>();
        map.put(treeRoot, 3);
        map.put(sender, 1);
        loadEstimates.put(addr1, map);
        peers.add(addr1);

        InetAddress addr2 = InetAddress.getByName("127.0.0.2");
        peers.add(addr2);

        Assert.assertEquals(addr1, thicket.findGraftAlternate(peers, loadEstimates));
    }

    @Test
    public void findGraftAlternate_BothUnknownLoadEst() throws UnknownHostException
    {
        List<InetAddress> peers = new LinkedList<>();

        ConcurrentMap<InetAddress, Map<InetAddress, Integer>> loadEstimates = new ConcurrentHashMap<>();
        InetAddress addr1 = InetAddress.getByName("127.0.0.1");
        peers.add(addr1);

        InetAddress addr2 = InetAddress.getByName("127.0.0.2");
        peers.add(addr2);

        InetAddress result = thicket.findGraftAlternate(peers, loadEstimates);
        // just make sure one of the expected values was returned
        Assert.assertTrue(result.equals(addr1) || result.equals(addr2));
    }

    static class AddressRecordingDispatcher implements GossipDispatcher<ThicketBroadcastService<ThicketMessage>, ThicketMessage>
    {
        public List<InetAddress> destinations = new ArrayList<>();
        public List<MessageWrapper> messages = new ArrayList<>();

        public void send(ThicketBroadcastService<ThicketMessage> svc, ThicketMessage msg, InetAddress dest)
        {
            destinations.add(dest);
            messages.add(new MessageWrapper(dest, msg));
        }
    }

    static class MessageWrapper
    {
        InetAddress addr;
        ThicketMessage msg;

        public MessageWrapper(InetAddress addr, ThicketMessage msg)
        {
            this.addr = addr;
            this.msg = msg;
        }
    }

    static class SimpleClient implements BroadcastClient
    {
        /** indicates if this message/messageId has been previously received */
        boolean freshMessage = true;
        String lastReceivedMessageId;
        String lastReceivedMessage;

        void isFreshMessage(boolean b)
        {
            freshMessage = b;
        }

        public String getClientId()
        {
            return "simple";
        }

        public boolean receiveBroadcast(Object messageId, Object message) throws IOException
        {
            lastReceivedMessageId = messageId.toString();
            lastReceivedMessage = message.toString();
            return freshMessage;
        }

        public Object prepareSummary()
        {
            return null;
        }

        public Set<? extends Object> receiveSummary(Object summary)
        {
            return null;
        }

        public boolean hasReceivedMessage(Object messageId)
        {
            return false;
        }

        public String toString()
        {
            return lastReceivedMessageId + ": " + lastReceivedMessage;
        }
    }
}
