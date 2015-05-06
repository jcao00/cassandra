package org.apache.cassandra.gms2.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms2.gossip.BroadcastClient;
import org.apache.cassandra.gms2.gossip.peersampling.PeerSamplingService;
import org.apache.cassandra.gms2.gossip.peersampling.PennStationDispatcher;
import org.apache.cassandra.gms2.gossip.thicket.MinimalPeerSamplingService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.ThicketConfigImpl;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;

public class ThicketIntegrationTest
{
    private static final Logger logger = LoggerFactory.getLogger(ThicketIntegrationTest.class);

    @Test
    public void simple() throws UnknownHostException
    {
        PennStationDispatcher<ThicketBroadcastService<ThicketMessage>, ThicketMessage> dispatcher = new PennStationDispatcher();
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        ThicketBroadcastService<ThicketMessage> thicket = new ThicketBroadcastService<>(new ThicketConfigImpl(addr), dispatcher);

        int cnt = 1;
        PeerSamplingService peerSamplingService = createPeerSamplingService(cnt);
        thicket.registered(peerSamplingService);
        Assert.assertEquals(cnt, thicket.getBackupPeers().size());
    }

    MinimalPeerSamplingService createPeerSamplingService(int peerCount) throws UnknownHostException
    {
        List<InetAddress> peers = new ArrayList<>(peerCount);
        for (int i = 0; i < peerCount; i++)
            peers.add(InetAddress.getByName("127.0.0." + i));
        return new MinimalPeerSamplingService(peers);
    }

    private void waitForQuiesence(PennStationDispatcher dispatcher)
    {
        while (dispatcher.stillWorking())
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);

        Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
    }

    @Test
    /**
     * This is more of an integration test, so probably best to move it elsewhere.
     */
    public void simpleBroadcast() throws UnknownHostException
    {
        PennStationDispatcher<ThicketBroadcastService<ThicketMessage>, ThicketMessage> dispatcher = new PennStationDispatcher();
        List<ThicketBroadcastService<ThicketMessage>> thickets = createThickets(dispatcher, 2);
        ThicketBroadcastService<ThicketMessage> sender = thickets.get(0);
        Assert.assertEquals(InetAddress.getByName("127.0.0.0"), sender.getAddress());
        ThicketBroadcastService<ThicketMessage> receiver = thickets.get(1);
        Assert.assertNotSame(sender.getAddress(), receiver.getAddress());

        SimpleClient client = new SimpleClient();
        receiver.register(client);

        String msgId = "msg0";
        String msg = "hello, thicket!";
        sender.broadcast(client.getClientId(), msgId, msg);
        waitForQuiesence(dispatcher);
        Assert.assertEquals(client.toString(), msgId, client.lastReceivedMessageId);
        Assert.assertEquals(client.toString(), msg, client.lastReceivedMessage);
    }

    List<ThicketBroadcastService<ThicketMessage>> createThickets(PennStationDispatcher<ThicketBroadcastService<ThicketMessage>, ThicketMessage> dispatcher, int count) throws UnknownHostException
    {
        List<ThicketBroadcastService<ThicketMessage>> thickets = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            InetAddress addr = InetAddress.getByName("127.0.0." + i);
            ThicketBroadcastService<ThicketMessage> thicket = new ThicketBroadcastService<>(new ThicketConfigImpl(addr), dispatcher);
            dispatcher.register(addr, thicket);

            MinimalPeerSamplingService peerSamplingService = createPeerSamplingService(count);
            peerSamplingService.removePeer(addr);
            thicket.registered(peerSamplingService);

            int peerCount = count - 1;
            Assert.assertEquals(peerCount, thicket.getBackupPeers().size());
            Assert.assertEquals(addr, thicket.getAddress());
            thickets.add(thicket);
        }
        return thickets;
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
