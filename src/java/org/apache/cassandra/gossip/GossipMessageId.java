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
package org.apache.cassandra.gossip;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A unique identifier for each gossip message. A semi-implicit requirement for the gossip messages is that there exists
 * a "happens-before" relationship for each message from a given node. The nested {@link IdGenerator} provides
 * monotonicity for the {@link #id} field, and it's up to the rest of the program to guarantee monotonicity
 * for the {@link #epoch}.
 */
public class GossipMessageId implements Comparable<GossipMessageId>
{
    public static final IVersionedSerializer<GossipMessageId> serializer = new Serializer();

    private final int epoch;
    private final int id;

    public GossipMessageId(int epoch, int id)
    {
        this.epoch = epoch;
        this.id = id;
    }

    @Override
    public int compareTo(GossipMessageId other)
    {
        if (epoch == other.epoch)
        {
            if (id == other.id)
                return 0;
            return id < other.id ? -1 : 1;
        }

        return epoch < other.epoch ? -1 : 1;
    }

    public int epochOnlyCompareTo(GossipMessageId other)
    {
        return epoch == other.epoch ? 0 : epoch < other.epoch ? -1 : 1;
    }

    public int getEpoch()
    {
        return epoch;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        int result = 17;
        result = 37 * result + epoch;
        result = 37 * result + id;
        return result;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof GossipMessageId))
            return false;
        GossipMessageId msgId = (GossipMessageId)o;
        return epoch == msgId.epoch && id == msgId.id;
    }

    @Override
    public String toString()
    {
        return epoch + ":" + id;
    }

    /**
     * A handy and simple generator for {@link GossipMessageId} instances for the current running instance.
     */
    public static class IdGenerator
    {
        private final AtomicInteger id = new AtomicInteger();
        private final int epoch;

        public IdGenerator(int epoch)
        {
            this.epoch = epoch;
        }

        public GossipMessageId generate()
        {
            return new GossipMessageId(epoch, id.getAndIncrement());
        }
    }

    private static class Serializer implements IVersionedSerializer<GossipMessageId>
    {
        public void serialize(GossipMessageId messageId, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(messageId.epoch);
            out.writeInt(messageId.id);
        }

        public long serializedSize(GossipMessageId messageId, int version)
        {
            return TypeSizes.sizeof(messageId.epoch) + TypeSizes.sizeof(messageId.id);
        }

        public GossipMessageId deserialize(DataInputPlus in, int version) throws IOException
        {
            return new GossipMessageId(in.readInt(), in.readInt());
        }
    }
}
