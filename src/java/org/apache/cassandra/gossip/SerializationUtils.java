package org.apache.cassandra.gossip;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SerializationUtils
{
    public static void serialize(GossipMessageId messageId, DataOutputPlus out) throws IOException
    {
        out.writeInt(messageId.getEpoch());
        out.writeInt(messageId.getId());
    }

    public static int serializedSize(GossipMessageId messageId)
    {
        return TypeSizes.sizeof(messageId.getEpoch()) + TypeSizes.sizeof(messageId.getId());
    }

    public static GossipMessageId deserializeMessageId(DataInputPlus in) throws IOException
    {
        return new GossipMessageId(in.readInt(), in.readInt());
    }
}
