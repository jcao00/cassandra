package org.apache.cassandra.gossip.hyparview;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService.Verb;

public class MessageSerializerFactory
{
    public static final IVersionedSerializer<JoinMessage> JOIN_SERIALIZER = new JoinSerializer();
    public static final IVersionedSerializer<JoinResponseMessage> JOIN_RESPONSE_SERIALIZER = new JoinResponseSerializer();
    public static final IVersionedSerializer<ForwardJoinMessage> FORWARD_JOIN_SERIALIZER = new ForwardJoinSerializer();
    public static final IVersionedSerializer<NeighborRequestMessage> NEIGHBOR_REQUEST_SERAILIZER = new NeighborRequestSerializer();
    public static final IVersionedSerializer<NeighborResponseMessage> NEIGHBOR_RESPONSE_SERIALIZER = new NeighborResponseSerializer();
    public static final IVersionedSerializer<? extends HyParViewMessage> DISCONNECT_SERIALIZER = new DisconnectSerializer();

    public static /*<T extends HyParViewMessage>*/ MessageOut<HyParViewMessage> getSerializer(HyParViewMessage message)
    {
        switch (message.getMessageType())
        {
            case JOIN:              return new MessageOut<JoinMessage>(Verb.HYPARVIEW_JOIN, message, JOIN_SERIALIZER);
            case JOIN_RESPONSE:     return new MessageOut<T>(Verb.HYPARVIEW_JOIN_RESPONSE, message, JOIN_RESPONSE_SERIALIZER);
            case FORWARD_JOIN:      return new MessageOut<T>(Verb.HYPARVIEW_FORWARD_JOIN, message, FORWARD_JOIN_SERIALIZER);
            case NEIGHBOR_REQUEST:  return new MessageOut<T>(Verb.HYPARVIEW_NEIGHBOR_REQUEST, message, NEIGHBOR_REQUEST_SERAILIZER);
            case NEIGHBOR_RESPONSE: return new MessageOut<T>(Verb.HYPARVIEW_NEIGHBOR_RESPONSE, message, NEIGHBOR_RESPONSE_SERIALIZER);
            case DISCONNECT:        return new MessageOut<T>(Verb.HYPARVIEW_DISCONNECT, message, DISCONNECT_SERIALIZER);
            default:
                throw new IllegalArgumentException("unknown message type: " + message.getMessageType());
        }
    }

    private static class JoinSerializer implements IVersionedSerializer<JoinMessage>
    {
        public void serialize(JoinMessage joinMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public JoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(JoinMessage joinMessage, int version)
        {
            return 0;
        }
    }

    private static class JoinResponseSerializer implements IVersionedSerializer<JoinResponseMessage>
    {
        public void serialize(JoinResponseMessage joinResponseMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public JoinResponseMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(JoinResponseMessage joinResponseMessage, int version)
        {
            return 0;
        }
    }

    private static class ForwardJoinSerializer implements IVersionedSerializer<ForwardJoinMessage>
    {
        public void serialize(ForwardJoinMessage forwardJoinMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public ForwardJoinMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(ForwardJoinMessage forwardJoinMessage, int version)
        {
            return 0;
        }
    }

    private static class NeighborRequestSerializer implements IVersionedSerializer<NeighborRequestMessage>
    {
        public void serialize(NeighborRequestMessage neighborRequestMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public NeighborRequestMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(NeighborRequestMessage neighborRequestMessage, int version)
        {
            return 0;
        }
    }

    private static class NeighborResponseSerializer implements IVersionedSerializer<NeighborResponseMessage>
    {
        public void serialize(NeighborResponseMessage neighborResponseMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public NeighborResponseMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(NeighborResponseMessage neighborResponseMessage, int version)
        {
            return 0;
        }
    }

    private static class DisconnectSerializer implements IVersionedSerializer<DisconnectMessage>
    {
        public void serialize(DisconnectMessage disconnectMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public DisconnectMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        public long serializedSize(DisconnectMessage disconnectMessage, int version)
        {
            return 0;
        }
    }
}
