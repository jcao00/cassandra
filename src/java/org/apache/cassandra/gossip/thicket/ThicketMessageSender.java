package org.apache.cassandra.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.gossip.GossipMessageId;
import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.gossip.SerializationUtils;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class ThicketMessageSender implements MessageSender<ThicketMessage>
{
    public static final IVersionedSerializer<DataMessage> DATA_SERIALIZER = new DataSerializer();
    public static final IVersionedSerializer<SummaryMessage> SUMMARY_SERIALIZER = new SummarySerializer();
    public static final IVersionedSerializer<GraftMessage> GRAFT_SERIALIZER = new GraftSerializer();
    public static final IVersionedSerializer<PruneMessage> PRUNE_SERIALIZER = new PruneSerializer();
    private static final Map<String, IVersionedSerializer<?>> serializers = new HashMap<>();

    public void send(InetAddress destination, ThicketMessage message)
    {
        MessagingService.instance().sendOneWay(getSerializer(message), destination);
    }

    public static void addSerializer(BroadcastServiceClient client)
    {
        serializers.put(client.getClientName(), client.getSerializer());
    }

    private MessageOut getSerializer(ThicketMessage message)
    {
        switch (message.getMessageType())
        {
            case DATA: return new MessageOut<>(MessagingService.Verb.THICKET_DATA, (DataMessage)message, DATA_SERIALIZER);
            case SUMMARY: return new MessageOut<>(MessagingService.Verb.THICKET_SUMMARY, (SummaryMessage)message, SUMMARY_SERIALIZER);
            case GRAFT: return new MessageOut<>(MessagingService.Verb.THICKET_GRAFT, (GraftMessage)message, GRAFT_SERIALIZER);
            case PRUNE: return new MessageOut<>(MessagingService.Verb.THICKET_PRUNE, (PruneMessage)message, PRUNE_SERIALIZER);
            default:
                throw new IllegalArgumentException("unknown thicket message type: " + message.getMessageType());
        }
    }

    /**
     * Writes out the fields that every {@link ThicketMessage} must contain:
     * - message id
     * - sender's ip address
     * - collection of {@link LoadEstimate}s
     */
    static void serializeBaseFields(ThicketMessage message, DataOutputPlus out, int version) throws IOException
    {
        SerializationUtils.serialize(message.messageId, out);
        CompactEndpointSerializationHelper.serialize(message.sender, out);
        out.writeShort(message.estimates.size());
        for (LoadEstimate estimate : message.estimates)
        {
            CompactEndpointSerializationHelper.serialize(estimate.treeRoot, out);
            out.writeInt(estimate.load);
        }
    }

    static long serializedSizeBaseFields(ThicketMessage message, int version)
    {
        long size = SerializationUtils.serializedSize(message.messageId);
        size += CompactEndpointSerializationHelper.serializedSize(message.sender);
        size += 2; //size of load estimates
        for (LoadEstimate estimate : message.estimates)
        {
            size += CompactEndpointSerializationHelper.serializedSize(estimate.treeRoot);
            size += 4; // load is an int
        }
        return size;
    }

    static BaseMessageFields deserializeBaseFields(DataInputPlus in, int version) throws IOException
    {
        GossipMessageId messageId = SerializationUtils.deserializeMessageId(in);
        InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
        List<LoadEstimate> estimates = new LinkedList<>();
        int estimatesSize = in.readShort();
        for (int i = 0; i < estimatesSize; i++)
            estimates.add(new LoadEstimate(CompactEndpointSerializationHelper.deserialize(in), in.readInt()));
        return new BaseMessageFields(messageId, addr, estimates);
    }

    static class BaseMessageFields
    {
        final GossipMessageId messgeId;
        final InetAddress sender;
        private final Collection<LoadEstimate> estimates;

        private BaseMessageFields(GossipMessageId messgeId, InetAddress sender, Collection<LoadEstimate> estimates)
        {
            this.messgeId = messgeId;
            this.sender = sender;
            this.estimates = estimates;
        }
    }

    private static class DataSerializer implements IVersionedSerializer<DataMessage>
    {
        public void serialize(DataMessage dataMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(dataMessage, out, version);
            CompactEndpointSerializationHelper.serialize(dataMessage.treeRoot, out);
            out.writeUTF(dataMessage.client);
            getClientSerializer(dataMessage.client).serialize(dataMessage.payload, out, version);
        }

        private static IVersionedSerializer getClientSerializer(String client)
        {
            IVersionedSerializer serializer = serializers.get(client);
            if (serializer == null)
                throw new IllegalStateException(String.format("no serializer for client %s is defined", client));
            return serializer;
        }

        public long serializedSize(DataMessage dataMessage, int version)
        {
            long size = serializedSizeBaseFields(dataMessage, version);
            size += CompactEndpointSerializationHelper.serializedSize(dataMessage.treeRoot);
            size += TypeSizes.sizeof(dataMessage.client);

            size += getClientSerializer(dataMessage.client).serializedSize(dataMessage.payload, version);
            return size;
        }

        public DataMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);
            InetAddress treeRoot = CompactEndpointSerializationHelper.deserialize(in);
            String client = in.readUTF();
            Object payload = getClientSerializer(client).deserialize(in, version);

            return new DataMessage(fields.sender, fields.messgeId, treeRoot, payload, client, fields.estimates);
        }
    }

    private static class SummarySerializer implements IVersionedSerializer<SummaryMessage>
    {
        public void serialize(SummaryMessage summaryMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(summaryMessage, out, version);

            Map<InetAddress, Collection<GossipMessageId>> msgs = summaryMessage.receivedMessages.asMap();
            out.writeShort(msgs.size());
            for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : msgs.entrySet())
            {
                CompactEndpointSerializationHelper.serialize(entry.getKey(), out);
                out.writeShort(entry.getValue().size());
                for (GossipMessageId messageId : entry.getValue())
                    SerializationUtils.serialize(messageId, out);
            }
        }

        public long serializedSize(SummaryMessage summaryMessage, int version)
        {
            long size = 0;
            size += serializedSizeBaseFields(summaryMessage, version);

            Map<InetAddress, Collection<GossipMessageId>> msgs = summaryMessage.receivedMessages.asMap();
            size += 2; // size of receivedMessages's keys
            for (Map.Entry<InetAddress, Collection<GossipMessageId>> entry : msgs.entrySet())
            {
                size += CompactEndpointSerializationHelper.serializedSize(entry.getKey());
                size += 2; // count of messageIds
                for (GossipMessageId messageId : entry.getValue())
                    size += SerializationUtils.serializedSize(messageId);
            }

            return size;
        }

        public SummaryMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);

            Multimap<InetAddress, GossipMessageId> receivedMessages = HashMultimap.create();
            int keysSize = in.readShort();
            for (int i = 0; i < keysSize; i++)
            {
                InetAddress addr = CompactEndpointSerializationHelper.deserialize(in);
                int msgIdCount = in.readShort();
                for (int j = 0; j < msgIdCount; j++)
                    receivedMessages.put(addr, SerializationUtils.deserializeMessageId(in));
            }

            return new SummaryMessage(fields.sender, fields.messgeId, receivedMessages, fields.estimates);
        }
    }

    private static class GraftSerializer implements IVersionedSerializer<GraftMessage>
    {
        public void serialize(GraftMessage graftMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(graftMessage, out, version);
            out.writeShort(graftMessage.treeRoots.size());
            for (InetAddress addr : graftMessage.treeRoots)
                CompactEndpointSerializationHelper.serialize(addr, out);
        }

        public long serializedSize(GraftMessage graftMessage, int version)
        {
            long size = 0;
            size += serializedSizeBaseFields(graftMessage, version);
            size += 2; // size of tree-roots
            // can't know if ipv4 or ipv6, so have to check each one
            for (InetAddress addr : graftMessage.treeRoots)
                size += CompactEndpointSerializationHelper.serializedSize(addr);

            return size;
        }

        public GraftMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);

            List<InetAddress> treeRoots = new LinkedList<>();
            int treeRootsSize = in.readShort();
            for (int i = 0; i < treeRootsSize; i++)
                treeRoots.add(CompactEndpointSerializationHelper.deserialize(in));

            return new GraftMessage(fields.sender, fields.messgeId, treeRoots, fields.estimates);
        }
    }

    private static class PruneSerializer implements IVersionedSerializer<PruneMessage>
    {
        public void serialize(PruneMessage pruneMessage, DataOutputPlus out, int version) throws IOException
        {
            serializeBaseFields(pruneMessage, out, version);
            out.writeShort(pruneMessage.treeRoots.size());
            for (InetAddress addr : pruneMessage.treeRoots)
                CompactEndpointSerializationHelper.serialize(addr, out);
        }

        public long serializedSize(PruneMessage pruneMessage, int version)
        {
            long size = 0;
            size += serializedSizeBaseFields(pruneMessage, version);
            size += 2; // size of tree-roots
            // can't know if ipv4 or ipv6, so have to check each one
            for (InetAddress addr : pruneMessage.treeRoots)
                size += CompactEndpointSerializationHelper.serializedSize(addr);

            return size;
        }

        public PruneMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            BaseMessageFields fields = deserializeBaseFields(in, version);

            List<InetAddress> treeRoots = new LinkedList<>();
            int treeRootsSize = in.readShort();
            for (int i = 0; i < treeRootsSize; i++)
                treeRoots.add(CompactEndpointSerializationHelper.deserialize(in));

            return new PruneMessage(fields.sender, fields.messgeId, treeRoots, fields.estimates);
        }
    }
}
