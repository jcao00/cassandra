package org.apache.cassandra.gms2.membership;

import java.util.Map;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;

public class PeerState
{
    final Map<ApplicationState, VersionedValue> applicationStates;

    private volatile boolean isAlive;

    public PeerState()
    {
        applicationStates = new NonBlockingHashMap<>();
        isAlive = true;
    }
}
