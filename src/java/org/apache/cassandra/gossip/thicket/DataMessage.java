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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class DataMessage extends ThicketMessage
{
    public static final IVersionedSerializer<DataMessage> serializer = new Serializer();

    /**
     * A map of client name to the serializer for it's data payloads, which we'll need for serializing the {@link #payload}.
     */
    // TODO:JEB this is super lame-o, but sorta works for now
    static final ConcurrentHashMap<String, BroadcastServiceClient<?>> clients = new ConcurrentHashMap<>();

    /**
     * The root of the tree (that is, node) which first sent this message.
     */
    public final InetAddress treeRoot;

    public final Object payload;
    public final String client;

    /**
     * The number of nodes this message has already been passed through on it's traversal down the tree.
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

    @Override
    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }

    @Override
    public MessageOut<DataMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.THICKET_DATA, this, serializer);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append(super.toString());
        sb.append(", treeRoot: ").append(treeRoot);
        sb.append(", client: ").append(client);
        sb.append(", payload: ").append(payload);
        return sb.toString();
    }

    @Override
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

    private static class Serializer implements IVersionedSerializer<DataMessage>
    {
        @Override
        public void serialize(DataMessage dataMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(dataMessage, out, version);
            CompactEndpointSerializationHelper.serialize(dataMessage.treeRoot, out);
            out.writeUTF(dataMessage.client);
            getClientSerializer(dataMessage.client).serialize(dataMessage.payload, out, version);
        }

        private static IVersionedSerializer getClientSerializer(String client)
        {
            BroadcastServiceClient bcastClient = clients.get(client);
            if (bcastClient != null)
                return bcastClient.getSerializer();
            throw new IllegalStateException(String.format("no serializer for client %s is defined", client));
        }

        @Override
        public long serializedSize(DataMessage dataMessage, int version)
        {
            long size = serializedSizeBaseFields(dataMessage, version);
            size += CompactEndpointSerializationHelper.serializedSize(dataMessage.treeRoot);
            size += TypeSizes.sizeof(dataMessage.client);
            size += getClientSerializer(dataMessage.client).serializedSize(dataMessage.payload, version);
            return size;
        }

        @Override
        public DataMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            InetAddress treeRoot = CompactEndpointSerializationHelper.deserialize(in);
            String client = in.readUTF();
            Object payload = getClientSerializer(client).deserialize(in, version);

            return new DataMessage(fields.sender, fields.messgeId, treeRoot, payload, client, fields.estimates);
        }
    }
}
