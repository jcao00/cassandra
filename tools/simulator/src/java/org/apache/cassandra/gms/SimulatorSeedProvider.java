package org.apache.cassandra.gms;

import org.apache.cassandra.locator.SeedProvider;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimulatorSeedProvider implements SeedProvider
{
    private static final List<InetAddress> seeds = new ArrayList<>();

    public SimulatorSeedProvider(Map<String, String> args)
    {
        // need this ctor sig to keep DD/Config happy
        try
        {
            seeds.add(InetAddress.getByName("127.0.0.0"));
        }
        catch (UnknownHostException e)
        {
            //nop
        }
    }

    //hate this, but don't want to fuck w/ DD right now....
    public static void setSeeds(List<InetAddress> seeds)
    {
        seeds.clear();
        seeds.addAll(seeds);
    }

    public List<InetAddress> getSeeds()
    {
        return seeds;
    }
}
