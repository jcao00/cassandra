package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.peersampling.messages.HyParViewMessage;


public interface GossipDispatcher
{
    void send(HyParViewService svc, HyParViewMessage msg, InetAddress dest);
}
