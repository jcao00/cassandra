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

package org.apache.cassandra.net.async;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.FileRegion;
import io.netty.channel.MessageSizeEstimator;

/**
 * More or less a copy of netty's {@link io.netty.channel.DefaultMessageSizeEstimator}, but measures
 * our message sizes.
 */
public class SizeEstimator implements MessageSizeEstimator
{
    public static final int DEFAULT_UNKKNOWN_SIZE = 8;

    private final Handle handle;

    public SizeEstimator(int messagingProtocolVersion)
    {
        this (DEFAULT_UNKKNOWN_SIZE, messagingProtocolVersion);
    }

    public SizeEstimator(int unknownSize, int messagingProtocolVersion)
    {
        if (unknownSize < 0)
            throw new IllegalArgumentException("unknownSize: " + unknownSize + " (expected: >= 0)");

        handle = new HandleImpl(unknownSize, messagingProtocolVersion);
    }

    @Override
    public Handle newHandle()
    {
        return handle;
    }

    private static final class HandleImpl implements Handle
    {
        private final int unknownSize;
        private final int messagingProtocolVersion;

        private HandleImpl(int unknownSize, int messagingProtocolVersion)
        {
            this.unknownSize = unknownSize;
            this.messagingProtocolVersion = messagingProtocolVersion;
        }

        @Override
        public int size(Object msg)
        {
            if (msg instanceof QueuedMessage)
                return ((QueuedMessage)msg).message.serializedSize(messagingProtocolVersion);
            if (msg instanceof ByteBuf)
                return ((ByteBuf) msg).readableBytes();
            if (msg instanceof ByteBufHolder)
                return ((ByteBufHolder) msg).content().readableBytes();
            if (msg instanceof FileRegion)
                return 0;
            return unknownSize;
        }
    }
}
