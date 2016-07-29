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
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class SummaryMessage extends ThicketMessage
{
    public static final IVersionedSerializer<SummaryMessage> serializer = new Serializer();

    final Multimap<InetAddress, GossipMessageId> receivedMessages;

    public SummaryMessage(InetAddress sender, GossipMessageId messageId, Multimap<InetAddress, GossipMessageId> receivedMessages, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.receivedMessages = receivedMessages;
    }

    @Override
    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.SUMMARY;
    }

    @Override
    public MessageOut<SummaryMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.THICKET_SUMMARY, this, serializer);
    }

    @Override
    public String toString()
    {
        return String.format("%s, receivedMessages: %s", super.toString(), receivedMessages);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof SummaryMessage))
            return false;
        SummaryMessage msg = (SummaryMessage)o;
        return super.equals(o) && receivedMessages.equals(msg.receivedMessages);
    }

    private static class Serializer implements IVersionedSerializer<SummaryMessage>
    {
        @Override
        public void serialize(SummaryMessage summaryMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(summaryMessage, out, version);

            Map<InetAddress, Collection<GossipMessageId>> msgs = summaryMessage.receivedMessages.asMap();
            out.writeShort(msgs.size());
            for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : msgs.entrySet())
            {
                CompactEndpointSerializationHelper.serialize(entry.getKey(), out);
                out.writeShort(entry.getValue().size());
                for (GossipMessageId messageId : entry.getValue())
                    GossipMessageId.serializer.serialize(messageId, out, version);
            }
        }

        @Override
        public long serializedSize(SummaryMessage summaryMessage, int version)
        {
            long size = 0;
            size += serializedSizeBaseFields(summaryMessage, version);

            Map<InetAddress, Collection<GossipMessageId>> msgs = summaryMessage.receivedMessages.asMap();
            size += 2; // size of receivedMessages's keys
            for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : msgs.entrySet())
            {
                size += CompactEndpointSerializationHelper.serializedSize(entry.getKey());
                size += 2; // count of messageIds
                for (GossipMessageId messageId : entry.getValue())
                    size += GossipMessageId.serializer.serializedSize(messageId, version);
            }

            return size;
        }

        @Override
        public SummaryMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);

            Multimap<InetAddress, GossipMessageId> receivedMessages = HashMultimap.create();
            int keysSize = in.readShort();
            for (int i = 0; i < keysSize; i++)
            {
                InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
                int msgIdCount = in.readShort();
                for (int j = 0; j < msgIdCount; j++)
                    receivedMessages.put(addr, GossipMessageId.serializer.deserialize(in, version));
            }

            return new SummaryMessage(fields.sender, fields.messgeId, receivedMessages, fields.estimates);
        }
    }

}
