package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gossip.hyparview.HyParViewMessage;
import org.apache.cassandra.gossip.hyparview.MessageSerializerFactory;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * The implemenation of {@link MessageSender} that should be used at run-time, when running cassandra normally
 * (this is, when not testing).
 */
public class StandardMessageSender implements MessageSender
{
    public void send(InetAddress destination, HyParViewMessage message)
    {
        MessagingService.instance().sendOneWay(MessageSerializerFactory.getSerializer(message), destination);
    }
}
