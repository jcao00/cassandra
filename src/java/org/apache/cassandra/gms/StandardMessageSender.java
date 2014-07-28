package org.apache.cassandra.gms;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.net.InetAddress;

public class StandardMessageSender implements GossipDigestMessageSender
{
    public void sendOneWay(MessageOut message, InetAddress to, Gossiper sender)
    {
        MessagingService.instance().sendOneWay(message, to);
    }

    public void sendRR(MessageOut message, InetAddress to, IAsyncCallback callback, Gossiper sender)
    {
        MessagingService.instance().sendRR(message, to, callback);
    }

    public boolean blockUntilReady()
    {
        MessagingService.instance().waitUntilListening();
        return true;
    }
}
