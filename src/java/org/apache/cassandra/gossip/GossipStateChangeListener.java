/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.gossip;

import java.io.IOException;
import java.net.InetAddress;
import java.util.EnumSet;

import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.Pair;

/**
 * Listens to the state changes of the local node and schedules relevant updates for broadcast.
 * Conversely, listens to a {@link BroadcastService} for updates about peers.
 */
public class GossipStateChangeListener implements IEndpointStateChangeSubscriber, BroadcastServiceClient<Pair<InetAddress, EndpointState>>
{
    private static final IVersionedSerializer<Pair<InetAddress, EndpointState>> serializer = new AddressAndEndpointSerializer();
    private static final String CLIENT_NAME = "GOSSIP_LISTENER";

    /**
     * The collection of {@link ApplicationState}s we care about broadcasting; not all s,tate changes
     * are worthwhile to broadcast.
     */
    private static final EnumSet<ApplicationState> STATES = EnumSet.of(ApplicationState.STATUS, ApplicationState.DC, ApplicationState.RACK,
                                                                ApplicationState.INTERNAL_IP, ApplicationState.SEVERITY);
    private final InetAddress localAddress;
    private final BroadcastService broadcastService;
    private final GossipProvider gossipProvider;

    private double lastSeverityValue = -1;
    private HeartBeatState lastBroadcastedState;

    public GossipStateChangeListener(InetAddress localAddress, BroadcastService broadcastService,
                                     GossipProvider gossipProvider)
    {
        this.localAddress = localAddress;
        this.broadcastService = broadcastService;
        this.gossipProvider = gossipProvider;
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

        EndpointState epState = gossipProvider.getLocalState();
        if (epState == null)
            return;

        HeartBeatState currentHeartBeat = epState.getHeartbeatSnapshot();
        if (lastBroadcastedState != null && lastBroadcastedState.compareTo(currentHeartBeat) >= 0)
            return;
//        currentHeartBeat.updateHeartBeat();

        // reduce the SEVERITY broadcasts as the value is updated every second, and we dont't need to flood the cluster
        // if the value has not changed
        if (ApplicationState.SEVERITY == state && Double.parseDouble(value.value) != lastSeverityValue)
            return;

        broadcastService.broadcast(Pair.create(localAddress, epState), this);
        lastBroadcastedState = epState.getHeartbeatSnapshot();
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

    public boolean receive(Pair<InetAddress, EndpointState> payload)
    {
        return gossipProvider.receive(payload.left, payload.right);
    }

    public IVersionedSerializer<Pair<InetAddress, EndpointState>> getSerializer()
    {
        return serializer;
    }

    /**
     * Needed as EndpointState nor it's serializer retain the addr of the host that owns the EndopintState
     */
    private static class AddressAndEndpointSerializer implements IVersionedSerializer<Pair<InetAddress, EndpointState>>
    {
        public void serialize(Pair<InetAddress, EndpointState> pair, DataOutputPlus out, int version) throws IOException
        {
            CompactEndpointSerializationHelper.serialize(pair.left, out);
            EndpointState.serializer.serialize(pair.right, out, version);
        }

        public long serializedSize(Pair<InetAddress, EndpointState> pair, int version)
        {
            long size = CompactEndpointSerializationHelper.serializedSize(pair.left);
            size += EndpointState.serializer.serializedSize(pair.right, version);
            return size;
        }

        public Pair<InetAddress, EndpointState> deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
            EndpointState state = EndpointState.serializer.deserialize(in, version);
            return Pair.create(addr, state);
        }
    }

    /**
     * Abstraction to use {@link GossipStateChangeListener} without a hard, explicit dependency on {@link Gossiper}.
     */
    public interface GossipProvider
    {
        /**
         * Called on a node before sending updates about it's endpoint state
         */
        EndpointState getLocalState();

        boolean receive(InetAddress addr, EndpointState state);
    }

    public static class GossiperProvider implements GossipProvider
    {
        private final InetAddress localAddress;

        public GossiperProvider(InetAddress localAddress)
        {
            this.localAddress = localAddress;
        }

        public EndpointState getLocalState()
        {
            return Gossiper.instance.getEndpointStateForEndpoint(localAddress);
        }

        public boolean receive(InetAddress addr, EndpointState state)
        {
            return Gossiper.instance.acceptBroadcast(addr, state);
        }
    }
}
