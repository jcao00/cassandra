package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.antientropy.AntiEntropyService;

/**
 * Listen for changes in membership service, and use that as the basis for peer selection during anti-entropy sessions.
 * We use this to get a feed of all known nodes in the cluster, and filter out the ones we already know about from the
 * peer sampling service. This gives us a richer anti-entropy reconciliation, helps the cluster converge faster, and heal
 * any broken parts of the broadcast tree.
 */
public class PeerSubscriber implements IEndpointStateChangeSubscriber
{
    /**
     * mapping of address to datacenter
     */
    final ConcurrentMap<InetAddress, String> nodes;

    public PeerSubscriber()
    {
        nodes = new ConcurrentHashMap<>();
        // TODO: might want a way to snag all the existing entries, if any - maybe take parameter to Membership service
    }

    public int getClusterSize()
    {
        return nodes.size();
    }

    public Multimap<String, InetAddress> getNodes()
    {
        HashMultimap<String, InetAddress> map = HashMultimap.create();
        for (Map.Entry<InetAddress, String> entry : nodes.entrySet())
            map.put(entry.getValue(), entry.getKey());
        return map;
    }

    @VisibleForTesting
    public void addNodes(Map<InetAddress, String> peers)
    {
        nodes.putAll(peers);
    }

    /*
        IEndpointStateChangeSubscriber methods
     */

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        // nop - wait until node is alive
    }

    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // nop
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state.equals(ApplicationState.DC))
            nodes.put(endpoint, value.value);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        VersionedValue vv = state.getApplicationState(ApplicationState.DC);
        if (vv != null)
            nodes.put(endpoint, vv.value);
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        // TODO: should we call removeActivePeer() or neighborDown(), as well?
        // this would cover the case of having nodes in the active/backup peer sets
        // that are not in the peer sampling service's view
        nodes.remove(endpoint);
    }

    public void onRemove(InetAddress endpoint)
    {
        nodes.remove(endpoint);
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // TODO: maybe notify thicket that the peer bounced, and any open anti-entropy sessions are likely dead
    }
}
