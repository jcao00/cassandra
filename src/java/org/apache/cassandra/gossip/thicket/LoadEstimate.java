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
package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

/**
 * Represents the number of downstream peers ({@link #load}) in a given tree for a node.
 */
class LoadEstimate
{
    final InetAddress treeRoot;
    final int load;

    LoadEstimate(InetAddress treeRoot, int load)
    {
        this.treeRoot = treeRoot;
        this.load = load;
    }

    @Override
    public String toString()
    {
        return String.format("%s, %s", treeRoot, load);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof LoadEstimate))
            return false;
        LoadEstimate est = (LoadEstimate) o;
        return treeRoot.equals(est.treeRoot) && load == est.load;
    }

    @Override
    public int hashCode()
    {
        return treeRoot.hashCode() + (37 * load);
    }
}
