package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.peersampling.messages.HyParViewMessage;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;


public interface GossipDispatcher
{
    void send(HyParViewService svc, HyParViewMessage msg, InetAddress dest);

    void send(ThicketBroadcastService svc, ThicketMessage msg, InetAddress dest);
}
