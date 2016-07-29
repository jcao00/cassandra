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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class PruneMessage extends ThicketMessage
{
    public static final IVersionedSerializer<PruneMessage> serializer = new Serializer();

    final Collection<InetAddress> treeRoots;

    PruneMessage(InetAddress sender, GossipMessageId messageId, Collection<InetAddress> treeRoots, Collection<LoadEstimate> estimates)
    {
        super(sender, messageId, estimates);
        this.treeRoots = treeRoots;
    }

    @Override
    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.PRUNE;
    }

    @Override
    public MessageOut<PruneMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.THICKET_PRUNE, this, serializer);
    }

    @Override
    public String toString()
    {
        return String.format("%s, treeRoots: %s", super.toString(), treeRoots);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof PruneMessage))
            return false;
        PruneMessage msg = (PruneMessage)o;
        return super.equals(o) && treeRoots.equals(msg.treeRoots);
    }

    private static class Serializer implements IVersionedSerializer<PruneMessage>
    {
        @Override
        public void serialize(PruneMessage pruneMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(pruneMessage, out, version);
            out.writeShort(pruneMessage.treeRoots.size());
            for (InetAddress addr : pruneMessage.treeRoots)
                CompactEndpointSerializationHelper.serialize(addr, out);
        }

        @Override
        public long serializedSize(PruneMessage pruneMessage, int version)
        {
            long size = 0;
            size += serializedSizeBaseFields(pruneMessage, version);
            size += 2; // size of tree-roots
            // can't know if ipv4 or ipv6, so have to check each one
            for (InetAddress addr : pruneMessage.treeRoots)
                size += CompactEndpointSerializationHelper.serializedSize(addr);

            return size;
        }

        @Override
        public PruneMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);

            List<InetAddress> treeRoots = new LinkedList<>();
            int treeRootsSize = in.readShort();
            for (int i = 0; i < treeRootsSize; i++)
                treeRoots.add(CompactEndpointSerializationHelper.deserialize(in));

            return new PruneMessage(fields.sender, fields.messgeId, treeRoots, fields.estimates);
        }
    }
}
