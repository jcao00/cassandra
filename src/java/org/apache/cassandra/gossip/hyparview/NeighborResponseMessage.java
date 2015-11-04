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
package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;

public class NeighborResponseMessage extends HyParViewMessage
{
    public enum Result { ACCEPT, DENY }

    public final Result result;
    public final int neighborRequestsCount;

    public NeighborResponseMessage(GossipMessageId messgeId, InetAddress sender, String datacenter, Result result,
                                   int neighborRequestsCount, Optional<GossipMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
        this.result = result;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_RESPONSE;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(200);
        sb.append(super.toString());
        sb.append(", result ").append(result);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof NeighborResponseMessage))
            return false;
        NeighborResponseMessage msg = (NeighborResponseMessage)o;

        return result.equals(msg.result) && neighborRequestsCount == msg.neighborRequestsCount;
    }
}
