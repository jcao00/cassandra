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

import java.util.concurrent.atomic.AtomicInteger;

public class GossipMessageId implements Comparable<GossipMessageId>
{
    private final int epoch;
    private final int id;

    public GossipMessageId(int epoch, int id)
    {
        this.epoch = epoch;
        this.id = id;
    }

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

    public int hashCode()
    {
        int result = 17;
        result = 37 * result + epoch;
        result = 37 * result + id;
        return result;
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof GossipMessageId))
            return false;
        GossipMessageId msgId = (GossipMessageId)o;
        return epoch == msgId.epoch && id == msgId.id;
    }

    public String toString()
    {
        return epoch + ":" + id;
    }

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
}
