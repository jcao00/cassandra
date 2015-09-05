package org.apache.cassandra.gossip;

import java.net.InetAddress;

public interface PeerSamplingServiceListener
{
    //TODO: reconsider if we *really* need to pass along the datacenter in these calls
    // I'm still on the fence.

    void neighborUp(InetAddress addr, String datacenter);
    void neighborDown(InetAddress addr, String datacenter);
}
