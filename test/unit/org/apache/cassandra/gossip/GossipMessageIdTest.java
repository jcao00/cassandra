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
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class GossipMessageIdTest
{
    private static final int VERSION = MessagingService.current_version;

    @Test
    public void compareTo_Same()
    {
        GossipMessageId messgeId = new GossipMessageId(42, 9827);
        Assert.assertEquals(0, messgeId.compareTo(messgeId));
    }

    @Test
    public void compareTo_OlderEpoch()
    {
        int epoch = 42;
        GossipMessageId messgeId = new GossipMessageId(epoch, 9827);
        Assert.assertEquals(-1, messgeId.compareTo(new GossipMessageId(epoch + 1, 1)));
    }

    @Test
    public void compareTo_NewerEpoch()
    {
        int epoch = 42;
        GossipMessageId messgeId = new GossipMessageId(epoch, 9827);
        Assert.assertEquals(1, messgeId.compareTo(new GossipMessageId(epoch - 1, 23874273)));
    }

    @Test
    public void compareTo_OlderId()
    {
        int epoch = 23476234;
        int id = 4234102;
        GossipMessageId messgeId = new GossipMessageId(epoch, id);
        Assert.assertEquals(-1, messgeId.compareTo(new GossipMessageId(epoch, id + 1)));
    }

    @Test
    public void compareTo_NewerId()
    {
        int epoch = 23476234;
        int id = 4234102;
        GossipMessageId messgeId = new GossipMessageId(epoch, id);
        Assert.assertEquals(1, messgeId.compareTo(new GossipMessageId(epoch, id - 1)));
    }

    @Test
    public void equals()
    {
        int epoch = 134551;
        int id = 9123;
        GossipMessageId messageId = new GossipMessageId(epoch, id);
        Assert.assertEquals(messageId, new GossipMessageId(epoch, id));
        Assert.assertFalse(messageId.equals(new GossipMessageId(epoch + 1, id)));
        Assert.assertFalse(messageId.equals(new GossipMessageId(epoch, id - 10)));
    }

    @Test
    public void serializer_RoundTrip() throws IOException
    {
        GossipMessageId messageId = new GossipMessageId(2387234, 42);
        DataOutputBuffer out = new DataOutputBuffer(1024);
        GossipMessageId.serializer.serialize(messageId, out, VERSION);
        ByteBuffer buf = out.buffer();
        Assert.assertEquals(GossipMessageId.serializer.serializedSize(messageId, VERSION), buf.remaining());

        DataInputBuffer in = new DataInputBuffer(buf, false);
        GossipMessageId result = GossipMessageId.serializer.deserialize(in, VERSION);
        Assert.assertEquals(messageId, result);

    }
}
