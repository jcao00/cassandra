package org.apache.cassandra.gossip.thicket;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.cassandra.gossip.MessageSender;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

public class ThicketMessageSender implements MessageSender<ThicketMessage>
{
    public static final IVersionedSerializer<DataMessage> DATA_SERIALIZER = new DataSerializer();
    public static final IVersionedSerializer<SummaryMessage> SUMMARY_SERIALIZER = new SummarySerializer();
    public static final IVersionedSerializer<GraftMessage> GRAFT_SERIALIZER = new GraftSerializer();
    public static final IVersionedSerializer<PruneMessage> PRUNE_SERIALIZER = new PruneSerializer();

    public void send(InetAddress destination, ThicketMessage message)
    {
        MessagingService.instance().sendOneWay(getSerializer(message), destination);
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

    // TODO:JEB implement serializers (once we know what goes in the messages)
    private static class DataSerializer implements IVersionedSerializer<DataMessage>
    {
        public void serialize(DataMessage dataMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public long serializedSize(DataMessage dataMessage, int version)
        {
            return 0;
        }

        public DataMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }
    }

    private static class SummarySerializer implements IVersionedSerializer<SummaryMessage>
    {
        public void serialize(SummaryMessage summaryMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public long serializedSize(SummaryMessage summaryMessage, int version)
        {
            return 0;
        }

        public SummaryMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }
    }

    private static class GraftSerializer implements IVersionedSerializer<GraftMessage>
    {
        public void serialize(GraftMessage graftMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public long serializedSize(GraftMessage graftMessage, int version)
        {
            return 0;
        }

        public GraftMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }
    }

    private static class PruneSerializer implements IVersionedSerializer<PruneMessage>
    {
        public void serialize(PruneMessage pruneMessage, DataOutputPlus out, int version) throws IOException
        {

        }

        public long serializedSize(PruneMessage pruneMessage, int version)
        {
            return 0;
        }

        public PruneMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }
    }
}
