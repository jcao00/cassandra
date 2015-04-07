package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.net.InetAddress;
import java.util.List;

public class HPVConfigImpl implements HPVConfig
{
    private final InetAddress localAddr;
    private final List<InetAddress> seeds;

    public HPVConfigImpl(InetAddress localAddr, List<InetAddress> seeds)
    {
        this.localAddr = localAddr;
        this.seeds = seeds;
    }

    public int getShuffleInterval()
    {
        return 60;
    }

    public List<InetAddress> getSeeds()
    {
        return seeds;
    }

    public InetAddress getLocalAddr()
    {
        return localAddr;
    }

    public byte getActiveViewLength()
    {
        return 3;
    }

    public byte getPassiveViewLength()
    {
        return 10;
    }

    public byte getShuffleActiveViewCount()
    {
        return 2;
    }

    public byte getShufflePassiveViewCount()
    {
        return 7;
    }

    public byte getShuffleWalkLength()
    {
        return 2;
    }

    // DO NOT MAKE THIS VALUE EVEN!! ON clusters with 1 Seed, the seed just thrashes w/ DISCONNECTS
    public byte getActiveRandomWalkLength()
    {
        return 2;
    }

    public byte getPassiveRandomWalkLength()
    {
        return 1;
    }
}
