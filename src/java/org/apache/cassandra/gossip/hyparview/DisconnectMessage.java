package org.apache.cassandra.gossip.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;

import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class DisconnectMessage extends HyParViewMessage
{
    public static final IVersionedSerializer<DisconnectMessage> serializer = new DisconnectSerializer();

    public DisconnectMessage(GossipMessageId messgeId, InetAddress peer, String datacenter)
    {
        super(messgeId, peer, datacenter, Optional.<GossipMessageId>empty());
    }

    @Override
    HPVMessageType getMessageType()
    {
        return HPVMessageType.DISCONNECT;
    }

    @Override
    public MessageOut<DisconnectMessage> getMessageOut()
    {
        return new MessageOut<>(MessagingService.Verb.HYPARVIEW_DISCONNECT, this, serializer);
    }

    private static class DisconnectSerializer implements IVersionedSerializer<DisconnectMessage>
    {
        @Override
        public void serialize(DisconnectMessage disconnectMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(disconnectMessage, out, version);
        }

        @Override
        public long serializedSize(DisconnectMessage disconnectMessage, int version)
        {
            return serializedSizeBaseFields(disconnectMessage, version);
        }

        @Override
        public DisconnectMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            return new DisconnectMessage(fields.messgeId, fields.sender, fields.datacenter);
        }
    }
}
