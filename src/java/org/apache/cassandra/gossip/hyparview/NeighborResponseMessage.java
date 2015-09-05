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

public class NeighborResponseMessage extends HyParViewMessage
{
    public static final IVersionedSerializer<NeighborResponseMessage> serializer = new Serializer();

    public enum Result { ACCEPT, DENY }

    public final Result result;
    final int neighborRequestsCount;

    public NeighborResponseMessage(GossipMessageId messgeId, InetAddress sender, String datacenter, Result result,
                                   int neighborRequestsCount, Optional<GossipMessageId> lastDisconnect)
    {
        super(messgeId, sender, datacenter, lastDisconnect);
        this.result = result;
        this.neighborRequestsCount = neighborRequestsCount;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(200);
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

    HPVMessageType getMessageType()
    {
        return HPVMessageType.NEIGHBOR_RESPONSE;
    }

    public MessageOut<? extends HyParViewMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_NEIGHBOR_RESPONSE, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<NeighborResponseMessage>
    {
        public void serialize(NeighborResponseMessage neighborResponseMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(neighborResponseMessage, out, version);
            HyParViewMessage.serialize(neighborResponseMessage.lastDisconnect, out, version);
            out.writeByte(neighborResponseMessage.result.ordinal());
            out.writeByte(neighborResponseMessage.neighborRequestsCount);
        }

        public long serializedSize(NeighborResponseMessage neighborResponseMessage, int version)
        {
            long size = serializedSizeBaseFields(neighborResponseMessage, version);
            size += HyParViewMessage.serializedSize(neighborResponseMessage.lastDisconnect, version);
            size++; // result enum
            size++; // request count
            return size;
        }

        public NeighborResponseMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            Optional<GossipMessageId> disconnect = deserializeOptionalMessageId(in, version);
            NeighborResponseMessage.Result result = NeighborResponseMessage.Result.values()[in.readByte()];
            int requestCount = in.readByte();
            return new NeighborResponseMessage(fields.messgeId, fields.sender, fields.datacenter, result, requestCount, disconnect);
        }
    }
}
