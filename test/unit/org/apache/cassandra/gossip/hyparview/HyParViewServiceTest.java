package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.locator.SeedProvider;
import org.apache.cassandra.utils.Pair;

public class HyParViewServiceTest
{
    static final String LOCAL_DC = "dc0";
    static final String REMOTE_DC_1 = "dc1";
    static final String REMOTE_DC_2 = "dc2`";

    static InetAddress localNodeAddr;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        localNodeAddr = InetAddress.getByName("127.0.0.1");
    }

    HyParViewService buildService()
    {
        return buildService(Collections.emptyList());
    }

    HyParViewService buildService(List<InetAddress> seeds)
    {
        SeedProvider seedProvider = new TestSeedProvider(seeds);
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);

        hpvService.testInit(new TestMessageSender(), null);

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
}
