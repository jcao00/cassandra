package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

import org.apache.cassandra.gossip.MessageSender;

public class SimulationMessageSender implements MessageSender
{
    private final InetAddress senderAddress;
    private final PennStationDispatcher dispatcher;

    public SimulationMessageSender(InetAddress senderAddress, PennStationDispatcher dispatcher)
    {
        this.senderAddress = senderAddress;
        this.dispatcher = dispatcher;
    }

    public void send(InetAddress destinationAddr, HyParViewMessage message)
    {
        dispatcher.sendMessage(senderAddress, destinationAddr, message);
    }
}
