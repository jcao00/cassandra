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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class ByteBufDataOutputStreamPlus extends BufferedDataOutputStreamPlus
{
    private static final Logger logger = LoggerFactory.getLogger(ByteBufDataOutputStreamPlus.class);

    private final Channel channel;
    private final int bufferSize;

    /**
     * This *must* be the owning {@link ByteBuf} for the {@link BufferedDataOutputStreamPlus#buffer}
     */
    private ByteBuf currentBuf;

    protected ByteBufDataOutputStreamPlus(Channel channel, ByteBuf buffer, int bufferSize)
    {
        super(buffer.nioBuffer(0, bufferSize));
        this.channel = channel;
        this.currentBuf = buffer;
        this.bufferSize = bufferSize;
    }

    protected WritableByteChannel newDefaultChannel()
    {
        return new WritableByteChannel()
        {
            @Override
            public int write(ByteBuffer src) throws IOException
            {
                assert src == buffer;
                int size = src.position();
                doFlush(size);
                return size;
            }

            @Override
            public boolean isOpen()
            {
                return channel.isOpen();
            }

            @Override
            public void close()
            {
                // TODO:JEB impl this (?) - not sure if it needs to be implemented in this context
            }
        };
    }

    public static ByteBufDataOutputStreamPlus create(Channel channel, int bufferSize)
    {
        ByteBuf buf = channel.alloc().directBuffer(bufferSize, bufferSize);
        return new ByteBufDataOutputStreamPlus(channel, buf, bufferSize);
    }

    /**
     * Writes the buf directly to the backing {@link #channel}, without copying to the intermediate {@link #buffer}.
     */
    public void write(ByteBuf buf) throws IOException
    {
        doFlush(buffer.position());

        if (!waitUntilWritable(channel))
            throw new IOException("outbound channel was not writable");

        // the (possibly naive) assumption that we should always flush this buf
        channel.writeAndFlush(buf).addListener(future -> handleBuffer(future));
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        // flush the current backing write buffer only if there's any pending data
        if (buffer.position() > 0)
        {
            currentBuf.writerIndex(buffer.position());

            if (!waitUntilWritable(channel))
                throw new IOException("outbound channel was not writable");

            channel.writeAndFlush(currentBuf).addListener(future -> handleBuffer(future));
            currentBuf = channel.alloc().directBuffer(bufferSize, bufferSize);
            buffer = currentBuf.nioBuffer(0, bufferSize);
        }
    }

    protected boolean waitUntilWritable(Channel channel)
    {
        if (channel.isWritable())
            return true;

        // wait until the channel is writable again.
        SimpleCondition condition = new SimpleCondition();
        ChannelHandler writabilityHandler = new ChannelWritabiliityListsener(condition);
        channel.pipeline().addLast(writabilityHandler);
        try
        {
            condition.await(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            // TODO:JEB handle this better
            // nop
        }

        channel.pipeline().remove(writabilityHandler);
        if (!channel.isWritable())
        {
            logger.info("JEB:: waited on condition but channel is still unwritable");
            channel.close();
//            session.sessionFailed(); /// ?????
            return false;
        }
        return true;
    }

    private static class ChannelWritabiliityListsener extends ChannelDuplexHandler
    {
        private final SimpleCondition condition;

        private ChannelWritabiliityListsener(SimpleCondition condition)
        {
            this.condition = condition;
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
        {
            if (ctx.channel().isWritable())
                condition.signalAll();
        }
    }


    private void handleBuffer(Future<? super Void> future)
    {
        // TODO:JEB handle errors here!
        if (!future.isSuccess() && channel.isOpen())
        {
            logger.error("sending file block failed", future.cause());
//            channel.close();
//            session.sessionFailed(); /// ????????
        }
    }

    @Override
    public void close() throws IOException
    {
        doFlush(0);
        // calling close on the super's channel will close the netty channel
        super.channel.close();
        currentBuf.release();
        currentBuf = null;
        buffer = null;
    }
}
