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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Parses out individual messages from the incoming buffers. Each message, both header and payload, is incrementally built up
 * from the available input data, then passed to the {@link #messageConsumer}.
 *
 * Note: this class derives from {@link ByteToMessageDecoder} to take advantage of the {@link ByteToMessageDecoder.Cumulator}
 * behavior across {@link #decode(ChannelHandlerContext, ByteBuf, List)} invocations. That way we don't have to maintain
 * the not-fully consumed {@link ByteBuf}s.
 */
public class MessageInHandler extends ByteToMessageDecoder
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    /**
     * The default target for consuming deserialized {@link MessageIn}.
     */
    static final BiConsumer<MessageIn, Integer> MESSAGING_SERVICE_CONSUMER = (messageIn, id) -> MessagingService.instance().receive(messageIn, id);

    private enum State
    {
        READ_FIRST_CHUNK,

        /**
         * Required for versions less than {@link MessagingService#VERSION_40}.
         */
        @Deprecated
        READ_IP_ADDRESS,

        READ_VERB,
        READ_PARAMETERS_SIZE,
        READ_PARAMETERS_DATA,
        READ_PAYLOAD_SIZE,
        READ_PAYLOAD
    }

    /**
     * The byte count for magic, msg id, timestamp values.
     */
    @VisibleForTesting
    static final int FIRST_SECTION_BYTE_COUNT = 12;

    private final InetAddressAndPort peer;
    private final int messagingVersion;

    /**
     * Abstracts out depending directly on {@link MessagingService#receive(MessageIn, int)}; this makes tests more sane
     * as they don't require nor trigger the entire message processing circus.
     */
    private final BiConsumer<MessageIn, Integer> messageConsumer;

    private State state;
    private MessageHeader messageHeader;

    MessageInHandler(InetAddressAndPort peer, int messagingVersion)
    {
        this (peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
    }

    public MessageInHandler(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        this.peer = peer;
        this.messagingVersion = messagingVersion;
        this.messageConsumer = messageConsumer;
        state = State.READ_FIRST_CHUNK;
    }

    /**
     * For each new message coming in, builds up a {@link MessageHeader} instance incrementally. This method
     * attempts to deserialize as much header information as it can out of the incoming {@link ByteBuf}, and
     * maintains a trivial state machine to remember progress across invocations.
     */
    @SuppressWarnings("resource")
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
        try
        {
            while (true)
            {
                // an imperfect optimization around calling in.readableBytes() all the time
                int readableBytes = in.readableBytes();

                switch (state)
                {
                    case READ_FIRST_CHUNK:
                        if (readableBytes < FIRST_SECTION_BYTE_COUNT)
                            return;
                        MessagingService.validateMagic(in.readInt());
                        messageHeader = new MessageHeader();
                        messageHeader.messageId = in.readInt();
                        int messageTimestamp = in.readInt(); // make sure to read the sent timestamp, even if DatabaseDescriptor.hasCrossNodeTimeout() is not enabled
                        messageHeader.constructionTime = MessageIn.deriveConstructionTime(peer, messageTimestamp, ApproximateTime.currentTimeMillis());
                        state = State.READ_IP_ADDRESS;
                        readableBytes -= FIRST_SECTION_BYTE_COUNT;
                        // fall-through
                    case READ_IP_ADDRESS:
                        if (messagingVersion >= MessagingService.VERSION_40)
                        {
                            messageHeader.from = peer;
                        }
                        else
                        {
                            // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                            // first, check that we can actually read the size byte, then check if we can read that number of bytes.
                            // the "+ 1" is to make sure we have the size byte in addition to the serialized IP addr count of bytes in the buffer.
                            int serializedAddrSize = 0;
                            if (readableBytes < 1 || readableBytes < (serializedAddrSize = in.getByte(in.readerIndex()) + 1))
                                return;
                            messageHeader.from = CompactEndpointSerializationHelper.instance.deserialize(inputPlus, messagingVersion);
                            readableBytes -= serializedAddrSize;
                        }
                        state = State.READ_VERB;
                        // fall-through
                    case READ_VERB:
                        if (readableBytes < 4)
                            return;
                        messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                        state = State.READ_PARAMETERS_SIZE;
                        readableBytes -= 4;
                        // fall-through
                    case READ_PARAMETERS_SIZE:
                        if (messagingVersion >= MessagingService.VERSION_40)
                        {
                            long length = VIntCoding.readUnsignedVInt(in);
                            if (length < 0)
                                return;
                            messageHeader.parameterLength = (int) length;
                            readableBytes = in.readableBytes();
                        }
                        else
                        {
                            if (readableBytes < 4)
                                return;
                            messageHeader.parameterLength = in.readInt();
                            readableBytes -= 4;
                        }
                        messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                        state = State.READ_PARAMETERS_DATA;
                        // fall-through
                    case READ_PARAMETERS_DATA:
                        if (messageHeader.parameterLength > 0)
                        {
                            if (messagingVersion >= MessagingService.VERSION_40)
                            {
                                if (readableBytes < messageHeader.parameterLength)
                                    return;
                                readParameters(in, inputPlus, messageHeader.parameterLength, messageHeader.parameters);
                                readableBytes -= messageHeader.parameterLength;
                            }
                            else
                            {
                                if (!readParametersPre40(in, inputPlus, messageHeader.parameterLength, messageHeader.parameters))
                                    return;
                                readableBytes = in.readableBytes(); // we read an indeterminate number of bytes for the headers, so just ask the buffer again
                            }
                        }
                        state = State.READ_PAYLOAD_SIZE;
                        // fall-through
                    case READ_PAYLOAD_SIZE:
                        if (messagingVersion >= MessagingService.VERSION_40)
                        {
                            long length = VIntCoding.readUnsignedVInt(in);
                            if (length < 0)
                                return;
                            messageHeader.payloadSize = (int) length;
                            readableBytes = in.readableBytes();
                        }
                        else
                        {
                            if (readableBytes < 4)
                                return;
                            messageHeader.payloadSize = in.readInt();
                            readableBytes -= 4;
                        }
                        state = State.READ_PAYLOAD;
                        // fall-through
                    case READ_PAYLOAD:
                        if (readableBytes < messageHeader.payloadSize)
                            return;

                        // TODO consider deserailizing the messge not on the event loop
                        MessageIn<Object> messageIn = MessageIn.read(inputPlus, messagingVersion,
                                                                     messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                     messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);

                        if (messageIn != null)
                            messageConsumer.accept(messageIn, messageHeader.messageId);

                        state = State.READ_FIRST_CHUNK;
                        messageHeader = null;
                        break;
                    default:
                        throw new IllegalStateException("unknown/unhandled state: " + state);
                }
            }
        }
        catch (Exception e)
        {
            exceptionCaught(ctx, e);
        }
    }

    private void readParameters(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterLength, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption we have all the bytes required to read the headers
        final int endIndex = in.readerIndex() + parameterLength;
        while (in.readerIndex() < endIndex)
        {
            String key = DataInputStream.readUTF(inputPlus);
            ParameterType parameterType = ParameterType.byName.get(key);
            long valueLength =  VIntCoding.readUnsignedVInt(in);
            byte[] value = new byte[Ints.checkedCast(valueLength)];
            in.readBytes(value);
            try (DataInputBuffer buffer = new DataInputBuffer(value))
            {
                parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
            }
        }
    }

    /**
     * @return <code>true</code> if all the parameters have been read from the {@link ByteBuf}; else, <code>false</code>.
     */
    private boolean readParametersPre40(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption that map.size() is a constant time function (HashMap.size() is)
        while (parameters.size() < parameterCount)
        {
            if (!canReadNextParam(in))
                return false;

            String key = DataInputStream.readUTF(inputPlus);
            ParameterType parameterType = ParameterType.byName.get(key);
            byte[] value = new byte[in.readInt()];
            in.readBytes(value);
            try (DataInputBuffer buffer = new DataInputBuffer(value))
            {
                parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
            }
        }

        return true;
    }

    /**
     * Determine if we can read the next parameter from the {@link ByteBuf}. This method will *always* set the {@code in}
     * readIndex back to where it was when this method was invoked.
     *
     * NOTE: this function would be sooo much simpler if we included a parameters length int in the messaging format,
     * instead of checking the remaining readable bytes for each field as we're parsing it. c'est la vie ...
     */
    @VisibleForTesting
    static boolean canReadNextParam(ByteBuf in)
    {
        in.markReaderIndex();
        // capture the readableBytes value here to avoid all the virtual function calls.
        // subtract 6 as we know we'll be reading a short and an int (for the utf and value lengths).
        final int minimumBytesRequired = 6;
        int readableBytes = in.readableBytes() - minimumBytesRequired;
        if (readableBytes < 0)
            return false;

        // this is a tad invasive, but since we know the UTF string is prefaced with a 2-byte length,
        // read that to make sure we have enough bytes to read the string itself.
        short strLen = in.readShort();
        // check if we can read that many bytes for the UTF
        if (strLen > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }
        in.skipBytes(strLen);
        readableBytes -= strLen;

        // check if we can read the value length
        if (readableBytes < 4)
        {
            in.resetReaderIndex();
            return false;
        }
        int valueLength = in.readInt();
        // check if we read that many bytes for the value
        if (valueLength > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }

        in.resetReaderIndex();
        return true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("Got message from unknown table while reading from socket; closing", cause);
        else if (cause instanceof IOException)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("Unexpected exception caught in inbound channel pipeline from " + ctx.channel().remoteAddress(), cause);

        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.debug("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        ctx.fireChannelInactive();
    }

    // should ony be used for testing!!!
    @VisibleForTesting
    MessageHeader getMessageHeader()
    {
        return messageHeader;
    }

    /**
     * A simple struct to hold the message header data as it is being built up.
     */
    static class MessageHeader
    {
        int messageId;
        long constructionTime;
        InetAddressAndPort from;
        MessagingService.Verb verb;
        int payloadSize;

        Map<ParameterType, Object> parameters = Collections.emptyMap();

        /**
         * Length of the parameter data. If the message's version is {@link MessagingService#VERSION_40} or higher,
         * this value is the total number of header bytes; else, for legacy messaging, this is the number of
         * key/value entries in the header.
         */
        int parameterLength;
    }
}
