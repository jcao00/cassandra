package org.apache.cassandra.gms2.gossip.antientropy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.antientropy.messages.AntiEntropyMessage;
import org.apache.cassandra.gms2.membership.PeerSubscriber;

public class AntiEntropyServiceTest
{
    private static final String DC = "dc1";

    private AntiEntropyService antiEntropyService;
    private PeerSubscriber peerSubscriber;
    private InetAddress addr1, addr2, addr3, addr4;

    @Before
    public void setup() throws UnknownHostException
    {
        AEConfigImpl config = new AEConfigImpl(InetAddress.getByName("127.0.0.1"), DC);
        peerSubscriber = new PeerSubscriber();
        antiEntropyService = new AntiEntropyService(config, new AddressRecordingDispatcher(), peerSubscriber);
        addr1 = InetAddress.getByName("127.0.0.42");
        addr2 = InetAddress.getByName("127.0.0.182");
        addr3 = InetAddress.getByName("127.0.0.231");
        addr4 = InetAddress.getByName("127.0.0.77");
    }

    @Test
    public void selectPeer_NoPeers()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        Assert.assertNull(antiEntropyService.selectPeer(clusterNodes, new LinkedList<InetAddress>()));
    }

    @Test
    public void selectPeer_OneDc_NoFilters()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        clusterNodes.put("us-east-1", addr1);
        Assert.assertEquals(addr1, antiEntropyService.selectPeer(clusterNodes, new LinkedList<InetAddress>()));
    }

    @Test
    public void selectPeer_OneDc_WithFilters()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        clusterNodes.put(DC, addr1);
        clusterNodes.put(DC, addr2);
        List<InetAddress> fromPeerSamplingSvc = new LinkedList<>();
        fromPeerSamplingSvc.add(addr1);
        Assert.assertEquals(addr2, antiEntropyService.selectPeer(clusterNodes, fromPeerSamplingSvc));
    }

    @Test
    public void selectPeer_MultiDcsIncludingOwn_NoFilters()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        clusterNodes.put("us-east-1", addr1);
        clusterNodes.put("us-east-1", addr2);
        clusterNodes.put(DC, addr3);
        clusterNodes.put(DC, addr4);
        Assert.assertNotNull(antiEntropyService.selectPeer(clusterNodes, new LinkedList<InetAddress>()));
    }

    @Test
    public void selectPeer_MultiDcsIncludingOwn_WithFilters()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        clusterNodes.put("us-east-1", addr1);
        clusterNodes.put("us-east-1", addr2);
        clusterNodes.put(DC, addr3);
        clusterNodes.put(DC, addr4);

        List<InetAddress> fromPeerSamplingSvc = new LinkedList<>();
        fromPeerSamplingSvc.add(addr1);
        fromPeerSamplingSvc.add(addr4);
        InetAddress peer = antiEntropyService.selectPeer(clusterNodes, fromPeerSamplingSvc);
        Assert.assertTrue(peer.equals(addr2) || peer.equals(addr3));
    }

    @Test
    public void selectPeer_MultiDcsNotIncludingOwn_WithFilters()
    {
        Multimap<String, InetAddress> clusterNodes = HashMultimap.create();
        clusterNodes.put("us-east-1", addr1);
        clusterNodes.put("us-east-1", addr2);
        clusterNodes.put("us-west-1", addr3);
        clusterNodes.put("us-west-1", addr4);

        List<InetAddress> fromPeerSamplingSvc = new LinkedList<>();
        fromPeerSamplingSvc.add(addr2);
        fromPeerSamplingSvc.add(addr4);
        InetAddress peer = antiEntropyService.selectPeer(clusterNodes, fromPeerSamplingSvc);
        Assert.assertTrue(peer.equals(addr1) || peer.equals(addr3));
    }


    static class AddressRecordingDispatcher implements GossipDispatcher<AntiEntropyService<AntiEntropyMessage>, AntiEntropyMessage>
    {
        public List<InetAddress> destinations = new ArrayList<>();
        public List<MessageWrapper> messages = new ArrayList<>();

        public void send(AntiEntropyService<AntiEntropyMessage> svc, AntiEntropyMessage msg, InetAddress dest)
        {
            destinations.add(dest);
            messages.add(new MessageWrapper(dest, msg));
        }
    }

    static class MessageWrapper
    {
        final InetAddress addr;
        final AntiEntropyMessage msg;

        public MessageWrapper(InetAddress addr, AntiEntropyMessage msg)
        {
            this.addr = addr;
            this.msg = msg;
        }
    }
}
