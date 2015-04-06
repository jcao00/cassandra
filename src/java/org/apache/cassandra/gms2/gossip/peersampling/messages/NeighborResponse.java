package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class NeighborResponse implements HyParViewMessage
{
    private static final ISerializer<NeighborResponse> serializer = new Serializer();

    public enum Result { ACCEPT, REJECT }

    private final Result result;

    public NeighborResponse(Result result)
    {
        this.result = result;
    }

    public Result getResult()
    {
        return result;
    }

    public MessageType getMessageType()
    {
        return MessageType.NEIGHBOR_RESPONSE;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<NeighborResponse>
    {
        public NeighborResponse deserialize(DataInput in) throws IOException
        {
            return new NeighborResponse(Result.values()[in.readByte()]);
        }

        public void serialize(NeighborResponse msg, DataOutputPlus out) throws IOException
        {
            out.writeByte(msg.getResult().ordinal());
        }

        public long serializedSize(NeighborResponse msg, TypeSizes type)
        {
            return 1;
        }
    }
}
