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

public abstract class HyParViewMessage
{
    public final GossipMessageId messageId;

    /**
     * Address of the peer that is sending/sent this message.
     */
    public final InetAddress sender;

    /**
     * Datacenter of the sender.
     */
    public final String datacenter;

    /**
     * The message id of the last disconnect the sender has received from the recipient.
     */
    public final Optional<GossipMessageId> lastDisconnect;

    protected HyParViewMessage(GossipMessageId messageId, InetAddress sender, String datacenter, Optional<GossipMessageId> lastDisconnect)
    {
        this.messageId = messageId;
        this.sender = sender;
        this.datacenter = datacenter;
        this.lastDisconnect = lastDisconnect;
    }

    public abstract HPVMessageType getMessageType();

    public String toString()
    {
        StringBuffer sb = new StringBuffer(128);
        sb.append(getMessageType()).append(", msgId: ").append(messageId);
        sb.append(", from ").append(sender).append(" (").append(datacenter).append(")");
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof HyParViewMessage))
            return false;
        HyParViewMessage msg = (HyParViewMessage)o;

        return messageId.equals(msg.messageId) &&
               sender.equals(msg.sender) &&
               datacenter.equals(msg.datacenter) &&
               getOriginator().equals(msg.getOriginator()) &&
               getOriginatorDatacenter().equals(msg.getOriginatorDatacenter()) &&
               getOriginatorMessageId().equals(msg.getOriginatorMessageId()) &&
               lastDisconnect.equals(msg.lastDisconnect);
    }

    public InetAddress getOriginator()
    {
        return sender;
    }

    public String getOriginatorDatacenter()
    {
        return datacenter;
    }

    public GossipMessageId getOriginatorMessageId()
    {
        return messageId;
    }
}
