package org.apache.cassandra.gossip.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

/**
 * The implemenation of {@link MessageSender} that should be used at run-time, when running cassandra normally
 * (this is, when not testing).
 */
public class HyParViewMessageSender implements MessageSender
{
    public static final IVersionedSerializer<JoinMessage> JOIN_SERIALIZER = new JoinSerializer();
    public static final IVersionedSerializer<JoinResponseMessage> JOIN_RESPONSE_SERIALIZER = new JoinResponseSerializer();
    public static final IVersionedSerializer<ForwardJoinMessage> FORWARD_JOIN_SERIALIZER = new ForwardJoinSerializer();
    public static final IVersionedSerializer<NeighborRequestMessage> NEIGHBOR_REQUEST_SERAILIZER = new NeighborRequestSerializer();
    public static final IVersionedSerializer<NeighborResponseMessage> NEIGHBOR_RESPONSE_SERIALIZER = new NeighborResponseSerializer();
    public static final IVersionedSerializer<DisconnectMessage> DISCONNECT_SERIALIZER = new DisconnectSerializer();

    public void send(InetAddress destination, HyParViewMessage message)
    {
        MessagingService.instance().sendOneWay(getSerializer(message), destination);
    }

    static MessageOut<? extends HyParViewMessage> getSerializer(HyParViewMessage message)
    {
        switch (message.getMessageType())
        {
            case JOIN:              return new MessageOut<>(MessagingService.Verb.HYPARVIEW_JOIN, (JoinMessage)message, JOIN_SERIALIZER);
            case JOIN_RESPONSE:     return new MessageOut<>(MessagingService.Verb.HYPARVIEW_JOIN_RESPONSE, (JoinResponseMessage)message, JOIN_RESPONSE_SERIALIZER);
            case FORWARD_JOIN:      return new MessageOut<>(MessagingService.Verb.HYPARVIEW_FORWARD_JOIN, (ForwardJoinMessage)message, FORWARD_JOIN_SERIALIZER);
            case NEIGHBOR_REQUEST:  return new MessageOut<>(MessagingService.Verb.HYPARVIEW_NEIGHBOR_REQUEST, (NeighborRequestMessage)message, NEIGHBOR_REQUEST_SERAILIZER);
            case NEIGHBOR_RESPONSE: return new MessageOut<>(MessagingService.Verb.HYPARVIEW_NEIGHBOR_RESPONSE, (NeighborResponseMessage)message, NEIGHBOR_RESPONSE_SERIALIZER);
            case DISCONNECT:        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_DISCONNECT, (DisconnectMessage)message, DISCONNECT_SERIALIZER);
            default:
                throw new IllegalArgumentException("unknown message type: " + message.getMessageType());
        }
    }

    /**
     * Writes out the fields that every {@link HyParViewMessage} must contain:
     * - message id
     * - sender's ip address
     * - sender's datacenter
     */
    static void serializeBaseFields(HyParViewMessage message, DataOutputPlus out, int version) throws IOException
    {
        serialize(message.messageId, out);
        CompactEndpointSerializationHelper.serialize(message.sender, out);
        out.writeUTF(message.datacenter);
    }

    static long serializedSizeBaseFields(HyParViewMessage message, int version)
    {
        return serializedSize(message.messageId) +
               CompactEndpointSerializationHelper.serializedSize(message.sender) +
               TypeSizes.sizeof(message.datacenter);
    }

    static BaseMessageFields deserializeBaseFields(DataInputPlus in, int version) throws IOException
    {
        HPVMessageId messageId = deserializeMessageId(in);
        InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
        String datacenter = in.readUTF();
        return new BaseMessageFields(messageId, addr, datacenter);
    }

    // because we can't have tuples in java :(
    static class BaseMessageFields
    {
        final HPVMessageId messgeId;
        final InetAddress sender;
        final String datacenter;

        private BaseMessageFields(HPVMessageId messgeId, InetAddress sender, String datacenter)
        {
            this.messgeId = messgeId;
            this.sender = sender;
            this.datacenter = datacenter;
        }
    }

    /*
        methods for serializing the HPVMessageId
     */
    private static void serialize(HPVMessageId messageId, DataOutputPlus out) throws IOException
    {
        out.writeInt(messageId.getEpoch());
        out.writeInt(messageId.getId());
    }

    private static int serializedSize(HPVMessageId messageId)
    {
        return TypeSizes.sizeof(messageId.getEpoch()) + TypeSizes.sizeof(messageId.getId());
    }

    private static HPVMessageId deserializeMessageId(DataInputPlus in) throws IOException
    {
        return new HPVMessageId(in.readInt(), in.readInt());
    }

    /*
        methods for serializing the optional discconectMessageId field
     */
    private static void serialize(Optional<HPVMessageId> optionalMessageId, DataOutputPlus out) throws IOException
    {
        // write out an indicator that the disconnect msgId is present
        if (optionalMessageId.isPresent())
        {
            out.write(1);
            HyParViewMessageSender.serialize(optionalMessageId.get(), out);
        }
        else
        {
            out.write(0);
        }
    }

    private static int serializedSize(Optional<HPVMessageId> messageId)
    {
        // byte indicator if there's a last diconnect messageId
        int size = 1;
        if (messageId.isPresent())
            size += HyParViewMessageSender.serializedSize(messageId.get());
        return size;
    }

    private static Optional<HPVMessageId> deserializeOptionalMessageId(DataInputPlus in) throws IOException
    {
        int disconnectIndicator = in.readByte();
        if (disconnectIndicator == 1)
            return Optional.of(HyParViewMessageSender.deserializeMessageId(in));
        return Optional.empty();
    }

    /**
     * Serailizer for {@link JoinMessage}
     */
    private static class JoinSerializer implements IVersionedSerializer<JoinMessage>
    {
        public void serialize(JoinMessage joinMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(joinMessage, out, version);
        }

        public long serializedSize(JoinMessage joinMessage, int version)
        {
            return serializedSizeBaseFields(joinMessage, version);
        }

        public JoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            return new JoinMessage(fields.messgeId, fields.sender, fields.datacenter);
        }
    }

    /**
     * Serailizer for {@link JoinResponseMessage}
     */
    private static class JoinResponseSerializer implements IVersionedSerializer<JoinResponseMessage>
    {
        public void serialize(JoinResponseMessage joinResponseMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(joinResponseMessage, out, version);
            HyParViewMessageSender.serialize(joinResponseMessage.lastDisconnect, out);
        }

        public long serializedSize(JoinResponseMessage joinResponseMessage, int version)
        {
            return serializedSizeBaseFields(joinResponseMessage, version) +
                   HyParViewMessageSender.serializedSize(joinResponseMessage.lastDisconnect);
        }

        public JoinResponseMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = HyParViewMessageSender.deserializeBaseFields(in, version);
            Optional<HPVMessageId> disconnect = HyParViewMessageSender.deserializeOptionalMessageId(in);
            return new JoinResponseMessage(fields.messgeId, fields.sender, fields.datacenter, disconnect);
        }
    }

    /**
     * Serailizer for {@link ForwardJoinMessage}
     */
    private static class ForwardJoinSerializer implements IVersionedSerializer<ForwardJoinMessage>
    {
        public void serialize(ForwardJoinMessage forwardJoinMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(forwardJoinMessage, out, version);
            HyParViewMessageSender.serialize(forwardJoinMessage.getOriginatorMessageId(), out);
            CompactEndpointSerializationHelper.serialize(forwardJoinMessage.getOriginator(), out);
            out.writeUTF(forwardJoinMessage.getOriginatorDatacenter());
            out.write(forwardJoinMessage.timeToLive);
        }

        public long serializedSize(ForwardJoinMessage forwardJoinMessage, int version)
        {
            long size = serializedSizeBaseFields(forwardJoinMessage, version);
            size += HyParViewMessageSender.serializedSize(forwardJoinMessage.getOriginatorMessageId());
            size += CompactEndpointSerializationHelper.serializedSize(forwardJoinMessage.getOriginator());
            size += TypeSizes.sizeof(forwardJoinMessage.getOriginatorDatacenter());
            size += 1; // timeToLive field

            return size;
        }

        public ForwardJoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = HyParViewMessageSender.deserializeBaseFields(in, version);
            HPVMessageId originatorMsgId = HyParViewMessageSender.deserializeMessageId(in);
            InetAddress originator = CompactEndpointSerializationHelper.deserialize(in);
            String originatorDatacenter = in.readUTF();
            int timeToLove = in.readByte();

            return new ForwardJoinMessage(fields.messgeId, fields.sender, fields.datacenter,
                                          originator, originatorDatacenter, timeToLove, originatorMsgId);
        }
    }

    /**
     * Serailizer for {@link NeighborRequestMessage}
     */
    private static class NeighborRequestSerializer implements IVersionedSerializer<NeighborRequestMessage>
    {
        public void serialize(NeighborRequestMessage neighborRequestMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(neighborRequestMessage, out, version);
            HyParViewMessageSender.serialize(neighborRequestMessage.lastDisconnect, out);
            out.write(neighborRequestMessage.priority.ordinal());
            out.write(neighborRequestMessage.neighborRequestsCount);
        }

        public long serializedSize(NeighborRequestMessage neighborRequestMessage, int version)
        {
            long size = HyParViewMessageSender.serializedSizeBaseFields(neighborRequestMessage, version);
            size += HyParViewMessageSender.serializedSize(neighborRequestMessage.lastDisconnect);
            size++; // priority enum
            size++; // request count
            return size;
        }

        public NeighborRequestMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = HyParViewMessageSender.deserializeBaseFields(in, version);
            Optional<HPVMessageId> disconnect = HyParViewMessageSender.deserializeOptionalMessageId(in);
            NeighborRequestMessage.Priority priority = NeighborRequestMessage.Priority.values()[in.readByte()];
            int requestCount = in.readByte();
            return new NeighborRequestMessage(fields.messgeId, fields.sender, fields.datacenter, priority, requestCount, disconnect);
        }
    }

    /**
     * Serailizer for {@link NeighborResponseMessage}
     */
    private static class NeighborResponseSerializer implements IVersionedSerializer<NeighborResponseMessage>
    {
        public void serialize(NeighborResponseMessage neighborResponseMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(neighborResponseMessage, out, version);
            HyParViewMessageSender.serialize(neighborResponseMessage.lastDisconnect, out);
            out.write(neighborResponseMessage.result.ordinal());
            out.write(neighborResponseMessage.neighborRequestsCount);
        }

        public long serializedSize(NeighborResponseMessage neighborResponseMessage, int version)
        {
            long size = HyParViewMessageSender.serializedSizeBaseFields(neighborResponseMessage, version);
            size += HyParViewMessageSender.serializedSize(neighborResponseMessage.lastDisconnect);
            size++; // result enum
            size++; // request count
            return size;
        }

        public NeighborResponseMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = HyParViewMessageSender.deserializeBaseFields(in, version);
            Optional<HPVMessageId> disconnect = HyParViewMessageSender.deserializeOptionalMessageId(in);
            NeighborResponseMessage.Result result = NeighborResponseMessage.Result.values()[in.readByte()];
            int requestCount = in.readByte();
            return new NeighborResponseMessage(fields.messgeId, fields.sender, fields.datacenter, result, requestCount, disconnect);
        }
    }

    /**
     * Serailizer for {@link DisconnectMessage}
     */
    private static class DisconnectSerializer implements IVersionedSerializer<DisconnectMessage>
    {
        public void serialize(DisconnectMessage disconnectMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(disconnectMessage, out, version);
        }

        public long serializedSize(DisconnectMessage disconnectMessage, int version)
        {
            return serializedSizeBaseFields(disconnectMessage, version);
        }

        public DisconnectMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            return new DisconnectMessage(fields.messgeId, fields.sender, fields.datacenter);
        }
    }
}
