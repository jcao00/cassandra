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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;

/**
 * IncomingFileMessage is used to receive part (or the entrirety) of a SSTable data file.
 */
public class IncomingFileMessage extends StreamMessage
{
    public static final IVersionedSerializer<IncomingFileMessage> serializer = new IVersionedSerializer<IncomingFileMessage>()
    {
        public IncomingFileMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            FileMessageHeader header = FileMessageHeader.serializer.deserialize(in, version);
            return new IncomingFileMessage(null, header);
        }

        public void serialize(IncomingFileMessage incomingFileMessage, DataOutputPlus out, int version)
        {
            throw new UnsupportedOperationException("Not allowed to call serialize on an incoming file");
        }

        public long serializedSize(IncomingFileMessage incomingFileMessage, int version)
        {
            throw new UnsupportedOperationException();
        }
    };

    public final FileMessageHeader header;
    public SSTableMultiWriter sstable;

    public IncomingFileMessage(SSTableMultiWriter sstable, FileMessageHeader header)
    {
        super(header.planId, header.sessionIndex);
        this.header = header;
        this.sstable = sstable;
    }

    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }

    @Override
    public MessageOut<IncomingFileMessage> createMessageOut()
    {
        throw new UnsupportedOperationException("cannot create a MessageOut for an incoming file");
    }

    public Type getType()
    {
        return Type.FILE;
    }

    @Override
    public IVersionedSerializer<IncomingFileMessage> getSerializer()
    {
        return serializer;
    }
}

