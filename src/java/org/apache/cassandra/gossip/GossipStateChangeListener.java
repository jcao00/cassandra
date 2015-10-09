package org.apache.cassandra.gossip;

import java.net.InetAddress;
import java.util.EnumSet;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gossip.BroadcastService;
import org.apache.cassandra.gossip.BroadcastServiceClient;

/**
 * Listens to the state changes of the local node and schedules relevant updates for broadcast.
 * Conversely, listens to a {@link BroadcastService} for updates about peers.
 */
class GossipStateChangeListener implements IEndpointStateChangeSubscriber, BroadcastServiceClient
{
    private static final String CLIENT_NAME = "GOSSIP_LISTENER";

    /**
     * The collection of {@link ApplicationState}s we care about broadcasting; not all s,tate changes
     * are worthwhile to broadcast.
     */
    private static final EnumSet<ApplicationState> STATES = EnumSet.of(ApplicationState.STATUS, ApplicationState.DC, ApplicationState.RACK,
                                                                ApplicationState.INTERNAL_IP, ApplicationState.SEVERITY);
    private final InetAddress localAddress;
    private final BroadcastService broadcastService;

    private double lastSeverityValue = -1;

    public GossipStateChangeListener(InetAddress localAddress, BroadcastService broadcastService)
    {
        this.localAddress = localAddress;
        this.broadcastService = broadcastService;
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        // NOP
    }

    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // NOP
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        // all changes to local state *only* come through here
        if (!localAddress.equals(endpoint) || !STATES.contains(state))
            return;

        // optimize the SEVERITY broadcasts as the value is updated every second, and we dont't need to flood the cluster
        // is the value has not changed
        if (ApplicationState.SEVERITY == state && Double.parseDouble(value.value) != lastSeverityValue)
            return;

        // TODO:JEB create the payload object
        broadcastService.broadcast(value.version, );
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        // NOP
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        // NOP
    }

    public void onRemove(InetAddress endpoint)
    {
        // NOP
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // NOP
    }

    public String getClientName()
    {
        return CLIENT_NAME;
    }

    public boolean receive(Object messageId, Object payload)
    {
        return true;
    }
}
