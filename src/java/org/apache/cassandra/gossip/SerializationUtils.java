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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SerializationUtils
{
    public static void serialize(GossipMessageId messageId, DataOutputPlus out) throws IOException
    {
        out.writeInt(messageId.getEpoch());
        out.writeInt(messageId.getId());
    }

    public static int serializedSize(GossipMessageId messageId)
    {
        return TypeSizes.sizeof(messageId.getEpoch()) + TypeSizes.sizeof(messageId.getId());
    }

    public static GossipMessageId deserializeMessageId(DataInputPlus in) throws IOException
    {
        return new GossipMessageId(in.readInt(), in.readInt());
    }
}
