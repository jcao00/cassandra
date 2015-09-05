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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;

public abstract class HyParViewMessage
{
    final GossipMessageId messageId;

    /**
     * Address of the peer that sent ths message.
     */
    public final InetAddress sender;

    /**
     * Datacenter of the sender.
     */
    public final String datacenter;

    /**
     * The message id of the last disconnect the sender has received from the recipient.
     */
    final Optional<GossipMessageId> lastDisconnect;

    protected HyParViewMessage(GossipMessageId messageId, InetAddress sender, String datacenter, Optional<GossipMessageId> lastDisconnect)
    {
        this.messageId = messageId;
        this.sender = sender;
        this.datacenter = datacenter;
        this.lastDisconnect = lastDisconnect;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(128);
        sb.append(getClass().getName()).append(", msgId: ").append(messageId);
        sb.append(", from ").append(sender).append(" (").append(datacenter).append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof HyParViewMessage))
            return false;
        HyParViewMessage msg = (HyParViewMessage)o;

        return getMessageType() == msg.getMessageType() &&
               messageId.equals(msg.messageId) &&
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

   /*
        methods for serializing the optional discconectMessageId field
   */
    static void serialize(Optional<GossipMessageId> optionalMessageId, DataOutputPlus out, int version) throws IOException
    {
        // write out an indicator that the disconnect msgId is present
        if (optionalMessageId.isPresent())
        {
            out.writeByte(1);
            GossipMessageId.serializer.serialize(optionalMessageId.get(), out, version);
        }
        else
        {
            out.writeByte(0);
        }
    }

    static int serializedSize(Optional<GossipMessageId> messageId, int version)
    {
        // byte indicator if there's a last diconnect messageId
        int size = 1;
        if (messageId.isPresent())
            size += GossipMessageId.serializer.serializedSize(messageId.get(), version);
        return size;
    }

    static Optional<GossipMessageId> deserializeOptionalMessageId(DataInputPlus in, int version) throws IOException
    {
        int disconnectIndicator = in.readByte();
        if (disconnectIndicator == 1)
            return Optional.of(GossipMessageId.serializer.deserialize(in, version));
        return Optional.empty();
    }

    /**
     * Writes out the fields that every {@link HyParViewMessage} must contain:
     * - message id
     * - sender's ip address
     * - sender's datacenter
     */
    static void serializeBaseFields(HyParViewMessage message, DataOutputPlus out, int version) throws IOException
    {
        GossipMessageId.serializer.serialize(message.messageId, out, version);
        CompactEndpointSerializationHelper.serialize(message.sender, out);
        out.writeUTF(message.datacenter);
    }

    static long serializedSizeBaseFields(HyParViewMessage message, int version)
    {
        return GossipMessageId.serializer.serializedSize(message.messageId, version) +
               CompactEndpointSerializationHelper.serializedSize(message.sender) +
               TypeSizes.sizeof(message.datacenter);
    }

    static BaseMessageFields deserializeBaseFields(DataInputPlus in, int version) throws IOException
    {
        GossipMessageId messageId = GossipMessageId.serializer.deserialize(in, version);
        InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
        String datacenter = in.readUTF();
        return new BaseMessageFields(messageId, addr, datacenter);
    }

    abstract HPVMessageType getMessageType();

    // because we can't have tuples in java :(
    static class BaseMessageFields
    {
        final GossipMessageId messgeId;
        final InetAddress sender;
        final String datacenter;

        private BaseMessageFields(GossipMessageId messgeId, InetAddress sender, String datacenter)
        {
            this.messgeId = messgeId;
            this.sender = sender;
            this.datacenter = datacenter;
        }
    }

    public abstract MessageOut<? extends HyParViewMessage> getMessageOut();
}
