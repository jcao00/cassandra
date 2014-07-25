package org.apache.cassandra.gms;

import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.net.InetAddress;

public class StandardMessageSender implements GossipDigestMessageSender
{
    public void sendOneWay(MessageOut message, InetAddress to, Gossiper sender)
    {
        MessagingService.instance().sendOneWay(message, to);
    }
}
