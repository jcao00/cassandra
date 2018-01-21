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

package org.apache.cassandra.gms;

import java.net.InetAddress;

public class Node
{
    public final InetAddress address;
    public final int generation;
    public final int heartbeat;
    public final VersionedState status;
    public final VersionedState load;
    public final VersionedState schema;
    public final VersionedState dc;
    public final VersionedState rack;
    public final VersionedState releaseVersion;
    public final VersionedState internalIP;
    public final VersionedState rpcAddress;
    public final VersionedState severity;
    public final VersionedState netVersion;
    public final VersionedState hostId;
    public final VersionedState tokens;
    public final VersionedState rpcReady;

    private Node(InetAddress address, int generation, int heartbeat, VersionedState status, VersionedState load,
                 VersionedState schema, VersionedState dc, VersionedState rack, VersionedState releaseVersion,
                 VersionedState internalIP, VersionedState rpcAddress, VersionedState severity,
                 VersionedState netVersion, VersionedState hostId, VersionedState tokens, VersionedState rpcReady)
    {
        this.address = address;
        this.generation = generation;
        this.heartbeat = heartbeat;
        this.status = status;
        this.load = load;
        this.schema = schema;
        this.dc = dc;
        this.rack = rack;
        this.releaseVersion = releaseVersion;
        this.internalIP = internalIP;
        this.rpcAddress = rpcAddress;
        this.severity = severity;
        this.netVersion = netVersion;
        this.hostId = hostId;
        this.tokens = tokens;
        this.rpcReady = rpcReady;
    }

    public Node incrementHeartbeat()
    {
        return new Node(address, generation, heartbeat + 1, status, load, schema, dc, rack,
                        releaseVersion, internalIP, rpcAddress, severity, netVersion, hostId, tokens, rpcReady);
    }

    // sample "update*" function. perhaps pass in pre-built VersionedState, to excess garbage creation if we
    // get stuck in a CAS loop
    public Node updateStatus(String nextStatus, int version)
    {
        if (version <= status.version)
            return this;

        int nextHB = Math.max(heartbeat, version);
        VersionedState vs = new VersionedState(nextStatus, version);
        return new Node(address, generation, nextHB, vs, load, schema, dc, rack,
                        releaseVersion, internalIP, rpcAddress, severity, netVersion, hostId, tokens, rpcReady);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof Node))
            return false;
        Node n = (Node)o;

        // naive (and incorrect) implementation
        return address.equals(n.address) && generation == n.generation && heartbeat == n.heartbeat;
    }

    public int hashCode()
    {
        // naive (and incorrect) implementation
        int result = 31;
        result = 37 * result + address.hashCode();
        result = 37 * result + generation;
        result = 37 * result + heartbeat;
        return result;
    }

    public GossipDigest digest()
    {
        return new GossipDigest(address, generation, heartbeat);
    }

    public static boolean dominates(Node a, Node b)
    {
        if (a.generation > b.generation)
            return true;
        if (b.generation > a.generation)
            return false;

        // note: testing for domination might be a CPU waste (no garbage created, at least)
        return a.heartbeat > b.heartbeat &&
               VersionedState.dominates(a.status, b.status) &&
               VersionedState.dominates(a.load, b.load) &&
               VersionedState.dominates(a.schema, b.schema) &&
               VersionedState.dominates(a.dc, b.dc) &&
               VersionedState.dominates(a.rack, b.rack) &&
               VersionedState.dominates(a.releaseVersion, b.releaseVersion) &&
               VersionedState.dominates(a.internalIP, b.internalIP) &&
               VersionedState.dominates(a.rpcAddress, b.rpcAddress) &&
               VersionedState.dominates(a.severity, b.severity) &&
               VersionedState.dominates(a.netVersion, b.netVersion) &&
               VersionedState.dominates(a.hostId, b.hostId) &&
               VersionedState.dominates(a.tokens, b.tokens) &&
               VersionedState.dominates(a.rpcReady, b.rpcReady);
    }

    public Node merge(Node node)
    {
        if (!address.equals(node.address))
            throw new IllegalArgumentException("cannot merge two different nodes");

        // if one node has a higher generation, do not merge, just take it as-is
        if (generation > node.generation)
            return this;
        if (node.generation > generation)
            return node;

        return new Node(address, generation, Math.max(heartbeat, node.heartbeat),
                        VersionedState.max(status, node.status),
                        VersionedState.max(load, node.load),
                        VersionedState.max(schema, node.schema),
                        VersionedState.max(dc, node.dc),
                        VersionedState.max(rack, node.rack),
                        VersionedState.max(releaseVersion, node.releaseVersion),
                        VersionedState.max(internalIP, node.internalIP),
                        VersionedState.max(rpcAddress, node.rpcAddress),
                        VersionedState.max(severity, node.severity),
                        VersionedState.max(netVersion, node.netVersion),
                        VersionedState.max(hostId, node.hostId),
                        VersionedState.max(tokens, node.tokens),
                        VersionedState.max(rpcReady, node.rpcReady)
        );
    }
}
