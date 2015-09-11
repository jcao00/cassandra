package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.locator.SeedProvider;

public class HyParViewServiceTest
{
    static final String LOCAL_DC = "dc0";
    static final String REMOTE_DC_1 = "dc1";
    static final String REMOTE_DC_2 = "dc2`";

    static InetAddress localNodeAddr;
    static SeedProvider seedProvider;

    TestMessageSender messageSender;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        localNodeAddr = InetAddress.getByName("127.0.0.1");
        seedProvider = new TestSeedProvider(Collections.singletonList(localNodeAddr));
    }

    @Before
    public void setUp()
    {
        messageSender = new TestMessageSender();
    }

    @Test
    public void addToLocalActiveView_AddSelf()
    {
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);
        hpvService.addToLocalActiveView(localNodeAddr);
        Assert.assertEquals(0, hpvService.getPeers().size());
    }

    @Test
    public void addToLocalActiveView_AddOther() throws UnknownHostException
    {
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);
        hpvService.testInit(messageSender, null);
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToLocalActiveView(peer);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
    }

    @Test
    public void addToRemoteActiveView() throws UnknownHostException
    {
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);
        hpvService.testInit(messageSender, null);
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToRemoteActiveView(peer, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addToRemoteActiveView(peer2, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertFalse(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(peer2));
    }

    @Test
    public void addToRemoteActiveView_MultipleDCs() throws UnknownHostException
    {
        HyParViewService hpvService = new HyParViewService(localNodeAddr, LOCAL_DC, seedProvider, 2);
        hpvService.testInit(messageSender, null);
        InetAddress peer = InetAddress.getByName("127.0.0.2");
        hpvService.addToRemoteActiveView(peer, REMOTE_DC_1);
        Assert.assertEquals(1, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));

        InetAddress peer2 = InetAddress.getByName("127.0.0.3");
        hpvService.addToRemoteActiveView(peer2, REMOTE_DC_2);
        Assert.assertEquals(2, hpvService.getPeers().size());
        Assert.assertTrue(hpvService.getPeers().contains(peer));
        Assert.assertTrue(hpvService.getPeers().contains(peer2));
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
