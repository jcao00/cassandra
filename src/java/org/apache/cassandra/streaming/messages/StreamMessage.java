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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.stringtemplate.v4.misc.STMessage;

/**
 * StreamMessage is an abstract base class that every messages in streaming protocol inherit.
 */
public abstract class StreamMessage
{
    public enum Type
    {
        STREAM_INIT(0, false, StreamInitMessage.serializer),
        STREAM_INIT_ACK(1, false, StreamInitAckMessage.serializer),
        PREPARE_SYN(2, false, PrepareSynMessage.serializer),
        PREPARE_SYNACK(3, false, PrepareSynAckMessage.serializer),
        PREPARE_ACK(4, false, PrepareAckMessage.serializer),
        FILE(5, true, null),
        COMPLETE(6, false, CompleteMessage.serializer),
        RECEIVED(7, false, ReceivedMessage.serializer),
        SESSION_FAILED(8, false, SessionFailedMessage.serializer),
        KEEP_ALIVE(9, false, KeepAliveMessage.serializer);

        private static final IntObjectMap<Type> idToTypeMap = new IntObjectOpenHashMap<>(values().length);
        static
        {
            for (Type v : values())
                idToTypeMap.put(v.getId(), v);
        }

        private final int id;

        /**
         * Indicates if this {@link Type} represents a file transfer message.
         */
        private final boolean transfer;

        private final IVersionedSerializer<? extends StreamMessage> serializer;

        Type(int id, boolean transfer, IVersionedSerializer<? extends StreamMessage> serializer)
        {
            this.id = id;
            this.transfer = transfer;
            this.serializer = serializer;
        }

        public int getId()
        {
            return id;
        }

        public boolean isTransfer()
        {
            return transfer;
        }

        public IVersionedSerializer<? extends StreamMessage> getSerializer()
        {
            return serializer;
        }

        public static Type getById(int id) throws IllegalArgumentException
        {
            Type t = idToTypeMap.get(id);
            if (t != null)
                return t;
            throw new IllegalArgumentException("unknown streamng message type id: " + id);
        }
    }

    public final UUID planId;
    public final int sessionIndex;

    private transient volatile boolean sent = false;

    protected StreamMessage(UUID planId, int sessionIndex)
    {
        this.planId = planId;
        this.sessionIndex = sessionIndex;
    }

    public void sent()
    {
        sent = true;
    }

    public boolean wasSent()
    {
        return sent;
    }

    public abstract Type getType();

    public abstract IVersionedSerializer<? extends StreamMessage> getSerializer();

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static void serialize(StreamMessage msg, DataOutputPlus out, int version) throws IOException
    {
        UUIDSerializer.serializer.serialize(msg.planId, out, version);
        out.writeInt(msg.sessionIndex);
    }

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static int serializedSize(StreamMessage msg, int version)
    {
        return UUIDGen.UUID_LEN + 4;
    }

    /**
     * To be invoked by {@link IVersionedSerializer}s of derived classes.
     */
    protected static Pair<UUID, Integer> deserialize(DataInputPlus in, int version) throws IOException
    {
        UUID planId = UUIDSerializer.serializer.deserialize(in, version);
        int sessionIndex = in.readInt();
        return Pair.create(planId, sessionIndex);
    }

    @Override
    public String toString()
    {
        return String.format("%s - planId: %s, sessionIndex: %d", getClass().getSimpleName(), planId, sessionIndex);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof StreamMessage))
            return false;
        if (o == this)
            return true;
        StreamMessage msg = (StreamMessage)o;
        return planId.equals(msg.planId) && sessionIndex == msg.sessionIndex;
    }
}
