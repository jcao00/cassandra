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

import org.apache.cassandra.gossip.GossipMessageId;

public class DataMessage extends ThicketMessage
{
    public final InetAddress treeRoot;
    public final Object payload;
    public final String client;

    /**
     * The number of nodes this message has already been passed through on it's traversal
     * down the spanning tree.
     */
    public final int hopCount;

    public DataMessage(InetAddress sender, GossipMessageId messageId, InetAddress treeRoot, Object payload, String client, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoot = treeRoot;
        this.payload = payload;
        this.client = client;
        hopCount = 1;
    }

    public DataMessage(InetAddress sender, Collection<LoadEstimate> estimates, DataMessage message)
    {
        super(sender, message.messageId, estimates);
        treeRoot = message.treeRoot;
        payload = message.payload;
        client = message.client;
        hopCount = message.hopCount + 1;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(super.toString());
        sb.append(", treeRoot: ").append(treeRoot);
        sb.append(", client: ").append(client);
        sb.append(", payload: ").append(payload);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof DataMessage))
            return false;
        DataMessage msg = (DataMessage)o;
        return super.equals(o) &&
               treeRoot.equals(msg.treeRoot) &&
               client.equals(msg.client) &&
               payload.equals(msg.payload);
    }
}
