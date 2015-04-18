package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketDataMessage;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;

public class ThicketBroadcastServiceTest
{
    ThicketBroadcastService<ThicketMessage> thicket;
    InetAddress addr;
    InetAddress sender;

    // an arbitrary address to use as a tree root
    InetAddress treeRoot;

    @Before
    public void setup() throws UnknownHostException
    {
        addr = InetAddress.getByName("127.0.0.1");
        thicket = new ThicketBroadcastService<>(new ThicketConfigImpl(addr), new EmptyDispatcher());
        sender = InetAddress.getByName("127.0.0.2");
        treeRoot = InetAddress.getByName("127.10.13.0");
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
    public void handleData() throws IOException
    {
        SimpleClient client = new SimpleClient();
        thicket.register(client);

        String msgId = "msg0";
        String msg = "hello, thicket!";
        ThicketDataMessage thicketMessage = new ThicketDataMessage(sender, client.getClientId(), msgId, msg, new byte[0]);

        thicket.handleDataMessage(thicketMessage, sender);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);
    }

    static class EmptyDispatcher implements GossipDispatcher<ThicketBroadcastService<ThicketMessage>, ThicketMessage>
    {
        public void send(ThicketBroadcastService<ThicketMessage> svc, ThicketMessage msg, InetAddress dest)
        {
            //nop
        }
    }

    static class SimpleClient implements BroadcastClient
    {
        String lastReceivedMessageId;
        String lastReceivedMessage;

        public String getClientId()
        {
            return "simple";
        }

        public boolean receiveBroadcast(Object messageId, Object message) throws IOException
        {
            lastReceivedMessageId = messageId.toString();
            lastReceivedMessage = message.toString();
            return true;
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
