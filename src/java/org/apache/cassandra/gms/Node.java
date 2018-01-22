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
        return new GossipDigest(address, generation, maxVersion(this));
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

    // I'd actually like to memoize this as a final field in the ctor
    public static int maxVersion(Node node)
    {
        int maxVersion = node.heartbeat;

        // TODO this isn't really fool-proof is a new field is added :-/
        maxVersion = Math.max(maxVersion, node.status.version);
        maxVersion = Math.max(maxVersion, node.load.version);
        maxVersion = Math.max(maxVersion, node.schema.version);
        maxVersion = Math.max(maxVersion, node.dc.version);
        maxVersion = Math.max(maxVersion, node.rack.version);
        maxVersion = Math.max(maxVersion, node.releaseVersion.version);
        maxVersion = Math.max(maxVersion, node.internalIP.version);
        maxVersion = Math.max(maxVersion, node.rpcAddress.version);
        maxVersion = Math.max(maxVersion, node.severity.version);
        maxVersion = Math.max(maxVersion, node.netVersion.version);
        maxVersion = Math.max(maxVersion, node.hostId.version);
        maxVersion = Math.max(maxVersion, node.tokens.version);
        maxVersion = Math.max(maxVersion, node.rpcReady.version);

        return maxVersion;
    }

    public static class Builder
    {
        private InetAddress address;
        private int generation;
        private int heartbeat;
        private VersionedState status;
        private VersionedState load;
        private VersionedState schema;
        private VersionedState dc;
        private VersionedState rack;
        private VersionedState releaseVersion;
        private VersionedState internalIP;
        private VersionedState rpcAddress;
        private VersionedState severity;
        private VersionedState netVersion;
        private VersionedState hostId;
        private VersionedState tokens;
        private VersionedState rpcReady;

        public Builder(InetAddress addr, int generation)
        {
            this.address = addr;
            this.generation = generation;

            status = VersionedState.EMPTY_STATE;
            load = VersionedState.EMPTY_STATE;
            schema = VersionedState.EMPTY_STATE;
            dc = VersionedState.EMPTY_STATE;
            rack = VersionedState.EMPTY_STATE;
            releaseVersion = VersionedState.EMPTY_STATE;
            internalIP = VersionedState.EMPTY_STATE;
            rpcAddress = VersionedState.EMPTY_STATE;
            severity = VersionedState.EMPTY_STATE;
            netVersion = VersionedState.EMPTY_STATE;
            hostId = VersionedState.EMPTY_STATE;
            tokens = VersionedState.EMPTY_STATE;
            rpcReady = VersionedState.EMPTY_STATE;
        }

        public Builder(Node node)
        {
            address = node.address;
            generation = node.generation;
            heartbeat = node.heartbeat;

            status = node.status;
            load = node.load;
            schema = node.schema;
            dc = node.dc;
            rack = node.rack;
            releaseVersion = node.releaseVersion;
            internalIP = node.internalIP;
            rpcAddress = node.rpcAddress;
            severity = node.severity;
            netVersion = node.netVersion;
            hostId = node.hostId;
            tokens = node.tokens;
            rpcReady = node.rpcReady;
        }

        public Builder setAddress(InetAddress address)
        {
            this.address = address;
            return this;
        }

        public Builder setGeneration(int generation)
        {
            this.generation = generation;
            return this;
        }

        public Builder setHeartbeat(int heartbeat)
        {
            this.heartbeat = heartbeat;
            return this;
        }

        public Builder setStatus(VersionedState status)
        {
            this.status = status;
            return this;
        }

        public Builder setLoad(VersionedState load)
        {
            this.load = load;
            return this;
        }

        public Builder setSchema(VersionedState schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder setDc(VersionedState dc)
        {
            this.dc = dc;
            return this;
        }

        public Builder setRack(VersionedState rack)
        {
            this.rack = rack;
            return this;
        }

        public Builder setReleaseVersion(VersionedState releaseVersion)
        {
            this.releaseVersion = releaseVersion;
            return this;
        }

        public Builder setInternalIP(VersionedState internalIP)
        {
            this.internalIP = internalIP;
            return this;
        }

        public Builder setRpcAddress(VersionedState rpcAddress)
        {
            this.rpcAddress = rpcAddress;
            return this;
        }

        public Builder setSeverity(VersionedState severity)
        {
            this.severity = severity;
            return this;
        }

        public Builder setNetVersion(VersionedState netVersion)
        {
            this.netVersion = netVersion;
            return this;
        }

        public Builder setHostId(VersionedState hostId)
        {
            this.hostId = hostId;
            return this;
        }

        public Builder setTokens(VersionedState tokens)
        {
            this.tokens = tokens;
            return this;
        }

        public Builder setRpcReady(VersionedState rpcReady)
        {
            this.rpcReady = rpcReady;
            return this;
        }

        public Node build()
        {
            if (generation < 0 || heartbeat < 0)
                throw new IllegalStateException("generation and heartbeat values must not be negative");
            return new Node(address, generation, heartbeat, status, load, schema, dc, rack,
                            releaseVersion, internalIP, rpcAddress, severity, netVersion, hostId, tokens, rpcReady);
        }

    }
}
