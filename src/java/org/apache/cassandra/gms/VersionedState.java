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

package org.apache.cassandra.gms;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

// JEB: i don't like needing to have a String for everything. maybe make custom typed-versions for primitives
// (there will only be int, double, and string)
public class VersionedState
{
    public static final IVersionedSerializer serializer = new Serializer();

    public static final VersionedState EMPTY_STATE = new VersionedState(null, Integer.MIN_VALUE);

    public final String value;
    public final int version;

    VersionedState(String value, int version)
    {
        this.value = value;
        this.version = version;
    }

    static VersionedState max(VersionedState a, VersionedState b)
    {
        if (a.version == b.version)
        {
            // assumes that the value is the same!! it would be nice, if perhaps unsafe, to remove this assert
            assert a.value.equals(b.value);
            return a;
        }

        return a.version > b.version ? a : b;
    }

    static <T> boolean dominates(VersionedState a, VersionedState b)
    {
        return a.version >= b.version;
    }


    private static class Serializer implements IVersionedSerializer<VersionedState>
    {
        public void serialize(VersionedState value, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(outValue(value, version));
            out.writeInt(value.version);
        }

        private String outValue(VersionedState value, int version)
        {
            return value.value;
        }

        public VersionedState deserialize(DataInputPlus in, int version) throws IOException
        {
            String value = in.readUTF();
            int valVersion = in.readInt();
            return new VersionedState(value, valVersion);
        }

        public long serializedSize(VersionedState value, int version)
        {
            return TypeSizes.sizeof(outValue(value, version)) + TypeSizes.sizeof(value.version);
        }
    }
}

