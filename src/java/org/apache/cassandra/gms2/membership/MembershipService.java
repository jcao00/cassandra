package org.apache.cassandra.gms2.membership;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.service.IEndpointLifecycleSubscriber;

public class MembershipService
{
    // TODO: in the future, this will be the ORSWOT CRDT, but a placeholder for now
    private final Orswot<> members;

    private final Map<InetSocketAddress, PeerState> peerStateMap;
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers;

    public MembershipService()
    {
//        members = new HashSet<>();
        lifecycleSubscribers = new ArrayList<>(4);
        peerStateMap = new ConcurrentHashMap<>();
    }

    //TODO: when size of membership changes, callback to the Broadcast service (HPV)
    // so it can adjust it's active/passive view size, ARWL/PRLW, and so on

    public void init()
    {

    }
}
