package org.apache.cassandra.gms2.gossip;

import java.net.InetAddress;

import org.apache.cassandra.gms2.gossip.peersampling.HyParViewService;
import org.apache.cassandra.gms2.gossip.peersampling.messages.HyParViewMessage;
import org.apache.cassandra.gms2.gossip.thicket.ThicketBroadcastService;
import org.apache.cassandra.gms2.gossip.thicket.messages.ThicketMessage;


public interface GossipDispatcher<S extends GossipDispatcher.GossipReceiver, M>
{
    void send(S svc, M msg, InetAddress dest);

    interface GossipReceiver<M>
    {
        void handle(M msg, InetAddress sender);

        InetAddress getAddress();
    }
}
