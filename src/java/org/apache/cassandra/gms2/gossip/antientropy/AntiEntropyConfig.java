package org.apache.cassandra.gms2.gossip.antientropy;

import java.net.InetAddress;

public interface AntiEntropyConfig
{
    InetAddress getAddress();

    String getDatacenter();
}
