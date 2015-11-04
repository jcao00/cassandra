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

public class ForwardJoinMessage extends HyParViewMessage
{
    /**
     * Address of the peer that is trying to join.
     */
    private final InetAddress originator;

    /**
     * The datacenter of the originator.
     */
    private final String originatorDatacenter;

    private final GossipMessageId originatorMessageId;

    /**
     * The number of steps remaining to forward the message. Not a TTL as in seconds.
     */
    public final int timeToLive;

    public ForwardJoinMessage(GossipMessageId messgeId, InetAddress sender, String senderDatacenter, InetAddress originator,
                              String originatorDatacenter, int timeToLive, GossipMessageId originatorId)
    {
        super(messgeId, sender, senderDatacenter, Optional.<GossipMessageId>empty());
        this.originator = originator;
        this.originatorDatacenter = originatorDatacenter;
        this.timeToLive = timeToLive;
        this.originatorMessageId = originatorId;
    }

    public HPVMessageType getMessageType()
    {
        return HPVMessageType.FORWARD_JOIN;
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer(200);
        sb.append(super.toString());
        sb.append(", originator ").append(originator).append(" (").append(originatorDatacenter).append(") ");
        return sb.toString();
    }

    public InetAddress getOriginator()
    {
        return originator;
    }

    public String getOriginatorDatacenter()
    {
        return originatorDatacenter;
    }

    public GossipMessageId getOriginatorMessageId()
    {
        return originatorMessageId;
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof ForwardJoinMessage))
            return false;
        ForwardJoinMessage msg = (ForwardJoinMessage)o;

        // all originator* fields should be checked in super.equals(), so just the custom fields here
        return timeToLive == msg.timeToLive;
    }
}
