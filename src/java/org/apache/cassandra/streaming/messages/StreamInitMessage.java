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
import java.net.InetAddress;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDSerializer;

/**
 * StreamInitMessage is first sent from the node where {@link org.apache.cassandra.streaming.StreamSession} is started,
 * to initiate corresponding {@link org.apache.cassandra.streaming.StreamSession} on the other side.
 */
public class StreamInitMessage extends StreamMessage
{
    public static Serializer<StreamInitMessage> serializer = new StreamInitMessageSerializer();

    public final InetAddress from;
    public final int sessionIndex;
    public final UUID planId;
    public final String description;

    public final boolean keepSSTableLevel;
    public final boolean isIncremental;
    public final UUID pendingRepair;

    public StreamInitMessage(InetAddress from, int sessionIndex, UUID planId, String description, boolean keepSSTableLevel, boolean isIncremental, UUID pendingRepair)
    {
        super(Type.STREAM_INIT);
        this.from = from;
        this.sessionIndex = sessionIndex;
        this.planId = planId;
        this.description = description;
        this.keepSSTableLevel = keepSSTableLevel;
        this.isIncremental = isIncremental;
        this.pendingRepair = pendingRepair;
    }

    private static class StreamInitMessageSerializer implements Serializer<StreamInitMessage>
    {
        public void serialize(StreamInitMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            CompactEndpointSerializationHelper.serialize(message.from, out);
            out.writeInt(message.sessionIndex);
            UUIDSerializer.serializer.serialize(message.planId, out, MessagingService.current_version);
            out.writeUTF(message.description);
            out.writeBoolean(message.keepSSTableLevel);
            out.writeBoolean(message.isIncremental);

            out.writeBoolean(message.pendingRepair != null);
            if (message.pendingRepair != null)
            {
                UUIDSerializer.serializer.serialize(message.pendingRepair, out, MessagingService.current_version);
            }
        }

        public StreamInitMessage deserialize(DataInputPlus in, int version, StreamSession session) throws IOException
        {
            InetAddress from = CompactEndpointSerializationHelper.deserialize(in);
            int sessionIndex = in.readInt();
            UUID planId = UUIDSerializer.serializer.deserialize(in, MessagingService.current_version);
            String description = in.readUTF();
            boolean keepSSTableLevel = in.readBoolean();

            boolean isIncremental = in.readBoolean();
            UUID pendingRepair = in.readBoolean() ? UUIDSerializer.serializer.deserialize(in, version) : null;
            return new StreamInitMessage(from, sessionIndex, planId, description, keepSSTableLevel, isIncremental, pendingRepair);
        }

        public long serializedSize(StreamInitMessage message, int version)
        {
            long size = CompactEndpointSerializationHelper.serializedSize(message.from);
            size += TypeSizes.sizeof(message.sessionIndex);
            size += UUIDSerializer.serializer.serializedSize(message.planId, MessagingService.current_version);
            size += TypeSizes.sizeof(message.description);
            size += TypeSizes.sizeof(message.keepSSTableLevel);
            size += TypeSizes.sizeof(message.isIncremental);
            size += TypeSizes.sizeof(message.pendingRepair != null);
            if (message.pendingRepair != null)
            {
                size += UUIDSerializer.serializer.serializedSize(message.pendingRepair, MessagingService.current_version);
            }
            return size;
        }
    }
}
