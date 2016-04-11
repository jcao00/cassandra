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
package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * @deprecated retry support removed on CASSANDRA-10992
 */
@Deprecated
public class RetryMessage extends StreamMessage
{
    public static final IVersionedSerializer<RetryMessage> serializer = new IVersionedSerializer<RetryMessage>()
    {
        public void serialize(RetryMessage msg, DataOutputPlus out, int version) throws IOException
        {
            StreamMessage.serialize(msg, out, version);
            UUIDSerializer.serializer.serialize(msg.cfId, out, MessagingService.current_version);
            out.writeInt(msg.sequenceNumber);
        }

        public RetryMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            Pair<UUID, Integer> header = StreamMessage.deserialize(in, version);
            return new RetryMessage(header.left, header.right, UUIDSerializer.serializer.deserialize(in, MessagingService.current_version), in.readInt());
        }

        public long serializedSize(RetryMessage msg, int version)
        {
            long size = StreamMessage.serializedSize(msg, version);
            size += UUIDGen.UUID_LEN;
            size += 4;
            return size;
        }
    };

    public final UUID cfId;
    public final int sequenceNumber;

    public RetryMessage(UUID planId, int sessionIndex, UUID cfId, int sequenceNumber)
    {
        super(planId, sessionIndex);
        this.cfId = cfId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("Retry (");
        sb.append(cfId).append(", #").append(sequenceNumber).append(')');
        return sb.toString();
    }

    @Override
    public MessageOut<RetryMessage> createMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.STREAM_RETRY, this, serializer);
    }

    public Type getType()
    {
        return Type.RETRY;
    }

    public IVersionedSerializer<? extends StreamMessage> getSerializer()
    {
        return serializer;
    }

    public boolean equals(Object o)
    {
        if (!super.equals(o) || !(o instanceof RetryMessage))
            return false;
        RetryMessage msg = (RetryMessage)o;
        return cfId.equals(msg.cfId) && sequenceNumber == msg.sequenceNumber;
    }
}
