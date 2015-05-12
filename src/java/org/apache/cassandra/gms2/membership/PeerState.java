package org.apache.cassandra.gms2.membership;

import java.util.Map;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;

public class PeerState
{
    // TODO: fix the type of the value
    final Map<ApplicationState, Integer> applicationStates;
    final long generation;

    // TODO: not sure this belongs here
    private volatile boolean isAlive;

    public PeerState(long generation)
    {
        this.generation = generation;
        applicationStates = new NonBlockingHashMap<>();
        isAlive = true;
    }

    public PeerState(long generation, Map<ApplicationState, Integer> states)
    {
        this.generation = generation;
        applicationStates = new NonBlockingHashMap<>();
        applicationStates.putAll(states);
        isAlive = true;
    }
}
