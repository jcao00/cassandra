package org.apache.cassandra.gms;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageOut;
import org.junit.Ignore;

import java.net.InetAddress;

@Ignore
public class NoOpGossipMessageSender implements GossipDigestMessageSender
{
    public void sendOneWay(MessageOut message, InetAddress to, Gossiper sender)
    {    }

    public void sendRR(MessageOut message, InetAddress to, IAsyncCallback callback, Gossiper sender)
    {    }

    public void blockUntilReady()
    {    }
}
