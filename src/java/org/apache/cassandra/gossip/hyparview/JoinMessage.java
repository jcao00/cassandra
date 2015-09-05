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

public class JoinMessage extends HyParViewMessage
{
    public static final IVersionedSerializer<JoinMessage> serializer = new Serializer();

    public JoinMessage(GossipMessageId messgeId, InetAddress sender, String datacenter)
    {
        super(messgeId, sender, datacenter, Optional.<GossipMessageId>empty());
    }

    @Override
    HPVMessageType getMessageType()
    {
        return HPVMessageType.JOIN;
    }

    @Override
    public MessageOut<? extends HyParViewMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_JOIN, this, serializer);
    }

    private static class Serializer implements IVersionedSerializer<JoinMessage>
    {
        @Override
        public void serialize(JoinMessage joinMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(joinMessage, out, version);
        }

        @Override
        public long serializedSize(JoinMessage joinMessage, int version)
        {
            return serializedSizeBaseFields(joinMessage, version);
        }

        @Override
        public JoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            return new JoinMessage(fields.messgeId, fields.sender, fields.datacenter);
        }
    }}
