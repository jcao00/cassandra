package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

import org.apache.cassandra.gossip.MessageSender;

public class BroadcastMessageSender implements MessageSender<ThicketMessage>
{
    private final InetAddress senderAddress;
    private final TimesSquareDispacher dispacher;

    public BroadcastMessageSender(InetAddress senderAddress, TimesSquareDispacher dispacher)
    {
        this.senderAddress = senderAddress;
        this.dispacher = dispacher;
    }

    public void send(InetAddress destination, ThicketMessage message)
    {
        dispacher.sendMessage(senderAddress, destination, message);
    }
}
