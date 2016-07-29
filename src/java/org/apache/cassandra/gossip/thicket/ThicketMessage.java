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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.hyparview.HyParViewMessage;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;

public abstract class ThicketMessage
{
    public final InetAddress sender;
    public final GossipMessageId messageId;
    public final Collection<LoadEstimate> estimates;

    protected ThicketMessage(InetAddress sender, GossipMessageId messageId, Collection<LoadEstimate> estimates)
    {
        this.sender = sender;
        this.messageId = messageId;
        this.estimates = estimates;
    }

    public abstract ThicketMessageType getMessageType();

    public abstract MessageOut<? extends ThicketMessage> getMessageOut();

    @Override
    public String toString()
    {
        return String.format("%s, sender: %s, msgId: %s", getMessageType(), sender, messageId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof ThicketMessage))
            return false;
        ThicketMessage msg = (ThicketMessage)o;
        return getMessageType() == msg.getMessageType() &&
               sender.equals(msg.sender) &&
               messageId.equals(msg.messageId) &&
               estimates.equals(msg.estimates);
    }

    /**
     * Writes out the fields that every {@link ThicketMessage} must contain:
     * - message id
     * - sender's ip address
     * - collection of {@link LoadEstimate}s
     */
    static void serializeBaseFields(ThicketMessage message, DataOutputPlus out, int version) throws IOException
    {
        GossipMessageId.serializer.serialize(message.messageId, out, version);
        CompactEndpointSerializationHelper.serialize(message.sender, out);
        out.writeShort(message.estimates.size());
        for (LoadEstimate estimate : message.estimates)
        {
            CompactEndpointSerializationHelper.serialize(estimate.treeRoot, out);
            out.writeInt(estimate.load);
        }
    }

    static long serializedSizeBaseFields(ThicketMessage message, int version)
    {
        long size = GossipMessageId.serializer.serializedSize(message.messageId, version);
        size += CompactEndpointSerializationHelper.serializedSize(message.sender);
        size += 2; //size of load estimates
        for (LoadEstimate estimate : message.estimates)
        {
            size += CompactEndpointSerializationHelper.serializedSize(estimate.treeRoot);
            size += 4; // load is an int
        }
        return size;
    }

    static BaseMessageFields deserializeBaseFields(DataInputPlus in, int version) throws IOException
    {
        GossipMessageId messageId = GossipMessageId.serializer.deserialize(in, version);
        InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
        List<LoadEstimate> estimates = new LinkedList<>();
        int estimatesSize = in.readShort();
        for (int i = 0; i < estimatesSize; i++)
            estimates.add(new LoadEstimate(CompactEndpointSerializationHelper.deserialize(in), in.readInt()));
        return new BaseMessageFields(messageId, addr, estimates);
    }

    protected static class BaseMessageFields
    {
        final GossipMessageId messgeId;
        final InetAddress sender;
        final Collection<LoadEstimate> estimates;

        private BaseMessageFields(GossipMessageId messgeId, InetAddress sender, Collection<LoadEstimate> estimates)
        {
            this.messgeId = messgeId;
            this.sender = sender;
            this.estimates = estimates;
        }
    }
}
