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
import java.util.Collection;
import java.util.Collection;

import org.apache.cassandra.gossip.GossipMessageId;

public class PruneMessage extends ThicketMessage
{
    public final Collection<InetAddress> treeRoots;

    public PruneMessage(InetAddress sender, GossipMessageId messageId, Collection<InetAddress> treeRoots, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoots = treeRoots;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }

    public String toString()
    {
        return String.format("%s, treeRoots: %s", super.toString(), treeRoots);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof PruneMessage))
            return false;
        PruneMessage msg = (PruneMessage)o;
        return super.equals(o) && treeRoots.equals(msg.treeRoots);
    }
}
