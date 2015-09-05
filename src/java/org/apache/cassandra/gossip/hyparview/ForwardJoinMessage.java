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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class ForwardJoinMessage extends HyParViewMessage
{
    public static final IVersionedSerializer<ForwardJoinMessage> serializer = new Serializer();

    /**
     * Address of the peer that is trying to join.
     */
    private final InetAddress originator;

    /**
     * The datacenter of the originator.
     */
    private final String originatorDatacenter;

    /**
     * The messageId that was sent by {@link #originator} when it initiated the join.
     */
    private final GossipMessageId originatorMessageId;

    /**
     * The number of steps remaining to forward the message. Not a TTL as in seconds, but in number of hops or
     * forwards to peers.
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(200);
        sb.append(super.toString());
        sb.append(", originator ").append(originator).append(" (").append(originatorDatacenter).append(") ");
        return sb.toString();
    }

    @Override
    public InetAddress getOriginator()
    {
        return originator;
    }

    @Override
    public String getOriginatorDatacenter()
    {
        return originatorDatacenter;
    }

    @Override
    public GossipMessageId getOriginatorMessageId()
    {
        return originatorMessageId;
    }

    @Override
    HPVMessageType getMessageType()
    {
        return HPVMessageType.FORWARD_JOIN;
    }

    @Override
    public MessageOut<? extends HyParViewMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_FORWARD_JOIN, this, serializer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof ForwardJoinMessage))
            return false;
        ForwardJoinMessage msg = (ForwardJoinMessage)o;

        // all originator* fields should be checked in super.equals(), so just the custom fields here
        return timeToLive == msg.timeToLive;
    }

    private static class Serializer implements IVersionedSerializer<ForwardJoinMessage>
    {
        @Override
        public void serialize(ForwardJoinMessage forwardJoinMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(forwardJoinMessage, out, version);
            GossipMessageId.serializer.serialize(forwardJoinMessage.getOriginatorMessageId(), out, version);
            CompactEndpointSerializationHelper.serialize(forwardJoinMessage.getOriginator(), out);
            out.writeUTF(forwardJoinMessage.getOriginatorDatacenter());
            out.write(forwardJoinMessage.timeToLive);
        }

        @Override
        public long serializedSize(ForwardJoinMessage forwardJoinMessage, int version)
        {
            long size = serializedSizeBaseFields(forwardJoinMessage, version);
            size += GossipMessageId.serializer.serializedSize(forwardJoinMessage.getOriginatorMessageId(), version);
            size += CompactEndpointSerializationHelper.serializedSize(forwardJoinMessage.getOriginator());
            size += TypeSizes.sizeof(forwardJoinMessage.getOriginatorDatacenter());
            size += 1; // timeToLive field

            return size;
        }

        @Override
        public ForwardJoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            GossipMessageId originatorMsgId = GossipMessageId.serializer.deserialize(in, version);
            InetAddress originator = CompactEndpointSerializationHelper.deserialize(in);
            String originatorDatacenter = in.readUTF();
            int timeToLive = in.readByte();

            return new ForwardJoinMessage(fields.messgeId, fields.sender, fields.datacenter,
                                          originator, originatorDatacenter, timeToLive, originatorMsgId);
        }
    }
}
