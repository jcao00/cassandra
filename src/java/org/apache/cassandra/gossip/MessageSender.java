package org.apache.cassandra.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gossip.hyparview.HyParViewMessage;

public interface MessageSender
{
    void send(InetAddress address, HyParViewMessage message);
}
