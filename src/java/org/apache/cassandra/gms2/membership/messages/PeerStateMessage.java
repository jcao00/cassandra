package org.apache.cassandra.gms2.membership.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.gms.ApplicationState;

// TODO: delete and merge w/ClusterMemberhsipMessage
public class PeerStateMessage
{
    private final InetAddress address;
    private final long generation;
    private final Map<ApplicationState, Integer> appStates;

    public PeerStateMessage(InetAddress address, long generation, Map<ApplicationState, Integer> appStates)
    {
        this.address = address;
        this.generation = generation;
        this.appStates = appStates;
    }

    public InetAddress getAddress()
    {
        return address;
    }

    public long getGeneration()
    {
        return generation;
    }

    public Map<ApplicationState, Integer> getAppStates()
    {
        return appStates;
    }
}
