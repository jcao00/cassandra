package org.apache.cassandra.gms2.gossip.thicket;

import java.net.InetAddress;

public interface ThicketConfig
{
    InetAddress getLocalAddr();
}
