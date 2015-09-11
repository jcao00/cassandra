package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gossip.hyparview.HyParViewMessage;
import org.apache.cassandra.net.MessagingService;

public class StandardMessageSender implements MessageSender
{
    public void send(InetAddress source, InetAddress desctination, HyParViewMessage message)
    {
        // TODO:JEB fix this
//        MessagingService.instance().sendRRWithFailure(, address, );
    }
}
