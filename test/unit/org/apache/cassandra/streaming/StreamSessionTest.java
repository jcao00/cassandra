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

package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Function;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingServiceTest;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier;
import org.apache.cassandra.streaming.async.NettyStreamingMessageSender;
import org.apache.cassandra.streaming.async.NettyStreamingMessageSenderFactory;

import static org.junit.Assert.assertEquals;

public class StreamSessionTest
{
    private static class NSMSStub extends NettyStreamingMessageSender
    {

        public NSMSStub(StreamSession session, OutboundConnectionIdentifier connectionId, StreamConnectionFactory factory, int protocolVersion, boolean isPreview)
        {
            super(session, connectionId, factory, protocolVersion, isPreview);
        }
    }

    @BeforeClass
    public static void beforeClass() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setBackPressureStrategy(new MessagingServiceTest.MockBackPressureStrategy(Collections.emptyMap()));
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.0.0.3"));
    }

    @Test
    public void testStreamSessionUsesCorrectRemoteIp_Succeeds() throws UnknownHostException
    {
        final OutboundConnectionIdentifier[] cid = new OutboundConnectionIdentifier[1];

        NettyStreamingMessageSenderFactory mockNSMSFactory = (session, connectionId, factory, protocolVersion, isPreview) -> {
            cid[0] = connectionId;
            return new NSMSStub(session, connectionId, factory, protocolVersion, isPreview);
        };

        Function<InetAddressAndPort, InetAddressAndPort> mockPeerIpMapper = (InetAddressAndPort peer) -> {
            try
            {
                return InetAddressAndPort.getByName("127.0.0.2:7000");
            }
            catch (UnknownHostException e)
            {
            }
            return null;
        };


        new StreamSession(StreamOperation.BOOTSTRAP, InetAddressAndPort.getByName("127.0.0.1:7000"),
                          new DefaultConnectionFactory(), 0, UUID.randomUUID(), PreviewKind.ALL, mockNSMSFactory,
                          mockPeerIpMapper);

        assertEquals(InetAddressAndPort.getByName("127.0.0.2:7000"), cid[0].remote());
    }

    @Test
    public void testStreamSessionUsesCorrectRemoteIpNullMapper_Succeeds() throws UnknownHostException
    {
        final OutboundConnectionIdentifier[] cid = new OutboundConnectionIdentifier[1];

        NettyStreamingMessageSenderFactory mockNSMSFactory = (session, connectionId, factory, protocolVersion, isPreview) -> {
            cid[0] = connectionId;
            return new NSMSStub(session, connectionId, factory, protocolVersion, isPreview);
        };

        new StreamSession(StreamOperation.BOOTSTRAP, InetAddressAndPort.getByName("127.0.0.1:7000"),
                          new DefaultConnectionFactory(), 0, UUID.randomUUID(), PreviewKind.ALL, mockNSMSFactory,
                          (peer) -> null);

        assertEquals(InetAddressAndPort.getByName("127.0.0.1:7000"), cid[0].remote());
    }
}
