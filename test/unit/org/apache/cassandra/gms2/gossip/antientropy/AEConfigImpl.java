package org.apache.cassandra.gms2.gossip.antientropy;

import java.net.InetAddress;

public class AEConfigImpl implements AntiEntropyConfig
{
    private final InetAddress addr;
    private final String datacenter;

    public AEConfigImpl(InetAddress addr, String datacenter)
    {
        this.addr = addr;
        this.datacenter = datacenter;
    }

    public InetAddress getAddress()
    {
        return addr;
    }

    public String getDatacenter()
    {
        return datacenter;
    }
}
