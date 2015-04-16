package org.apache.cassandra.gms2.gossip.thicket;

import java.net.InetAddress;

public class ThicketConfigImpl implements ThicketConfig
{
    private final InetAddress localAddr;

    public ThicketConfigImpl(InetAddress localAddr)
    {
        this.localAddr = localAddr;
    }
    public InetAddress getLocalAddr()
    {
        return localAddr;
    }
}
