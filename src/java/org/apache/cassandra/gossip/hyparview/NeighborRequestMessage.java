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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class NeighborRequestMessage extends HyParViewMessage
{
    public static final IVersionedSerializer<NeighborRequestMessage> serializer = new Serializer();

    enum Priority { HIGH, LOW }

    public final Priority priority;

    /**
     * A simple counter for the number of times the node has sent neighbor requests. This is used to stop sending
     * requests after a limited number of rejections.
     */
    final int neighborRequestsCount;

    public NeighborRequestMessage(GossipMessageId messgeId, InetAddress sender, String datacenter, Priority priority,
                                  int neighborRequestsCount, Optional<GossipMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
        this.priority = priority;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(200);
        sb.append(super.toString());
        sb.append(", priority ").append(priority);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof NeighborRequestMessage))
            return false;
        NeighborRequestMessage msg = (NeighborRequestMessage)o;

        return priority.equals(msg.priority) && neighborRequestsCount == msg.neighborRequestsCount;
    }

    @Override
    HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_REQUEST;
    }

    @Override
    public MessageOut<? extends HyParViewMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_NEIGHBOR_REQUEST, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<NeighborRequestMessage>
    {
        @Override
        public void serialize(NeighborRequestMessage neighborRequestMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(neighborRequestMessage, out, version);
            HyParViewMessage.serialize(neighborRequestMessage.lastDisconnect, out, version);
            out.writeByte(neighborRequestMessage.priority.ordinal());
            out.writeByte(neighborRequestMessage.neighborRequestsCount);
        }

        @Override
        public long serializedSize(NeighborRequestMessage neighborRequestMessage, int version)
        {
            long size = serializedSizeBaseFields(neighborRequestMessage, version);
            size += HyParViewMessage.serializedSize(neighborRequestMessage.lastDisconnect, version);
            size++; // priority enum
            size++; // request count
            return size;
        }

        @Override
        public NeighborRequestMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            Optional<GossipMessageId> disconnect = deserializeOptionalMessageId(in, version);
            NeighborRequestMessage.Priority priority = NeighborRequestMessage.Priority.values()[in.readByte()];
            int requestCount = in.readByte();
            return new NeighborRequestMessage(fields.messgeId, fields.sender, fields.datacenter, priority, requestCount, disconnect);
        }
    }
}
