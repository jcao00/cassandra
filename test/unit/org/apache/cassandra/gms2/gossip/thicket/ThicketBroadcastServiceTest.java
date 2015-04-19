package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.thicket.messages.MessageType;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketDataMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;

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
        addr = InetAddress.getByName("127.0.0.1");
        thicket = new ThicketBroadcastService<>(new ThicketConfigImpl(addr), new AddressRecordingDispatcher());
        sender = InetAddress.getByName("127.0.0.2");
        treeRoot = InetAddress.getByName("127.10.13.0");
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
    }

    @Test
    public void broadcast_WithNoPeers()
    {
        thicket.broadcast(client.getClientId(), msgId, msg);
        AddressRecordingDispatcher dispatcher = (AddressRecordingDispatcher)thicket.getDispatcher();
        Assert.assertEquals(dispatcher.destinations.toString(), 0, dispatcher.destinations.size());
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

        public void receiveSummary(Object summary)
        {

        }

        public String toString()
        {
            return lastReceivedMessageId + ": " + lastReceivedMessage;
        }
    }
}
