package org.apache.cassandra.gms2.membership.messages;

import java.net.InetAddress;
import java.util.Map;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms2.membership.OrswotClock;

// TODO: refactor this
public class ClusterMembershipMessage<A>
{
    public enum Action {ADD, REMOVE}

    private final InetAddress address;
    private final long generation;
    private final Map<ApplicationState, Integer> appStates;
    private final Action action;
    private final OrswotClock<A> clock;

    public ClusterMembershipMessage(InetAddress address, long generation, Map<ApplicationState, Integer> appStates, Action action, OrswotClock<A> clock)
    {
        this.address = address;
        this.generation = generation;
        this.appStates = appStates;
        this.action = action;
        this.clock = clock;
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

    public Action getAction()
    {
        return action;
    }

    public OrswotClock<A> getClock()
    {
        return clock;
    }
}
