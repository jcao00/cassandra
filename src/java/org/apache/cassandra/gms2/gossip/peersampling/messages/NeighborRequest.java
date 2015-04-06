package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class NeighborRequest implements HyParViewMessage
{
    private static final ISerializer<NeighborRequest> serializer = new Serializer();

    public enum Priority { HIGH, LOW }

    private final Priority priority;

    public NeighborRequest(Priority priority)
    {
        this.priority = priority;
    }

    public Priority getPriority()
    {
        return priority;
    }

    public MessageType getMessageType()
    {
        return MessageType.NEIGHBOR_REQUEST;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<NeighborRequest>
    {
        public NeighborRequest deserialize(DataInput in) throws IOException
        {
            return new NeighborRequest(Priority.values()[in.readByte()]);
        }

        public void serialize(NeighborRequest msg, DataOutputPlus out) throws IOException
        {
            out.writeByte(msg.getPriority().ordinal());
        }

        public long serializedSize(NeighborRequest msg, TypeSizes type)
        {
            return 1;
        }
    }
}
