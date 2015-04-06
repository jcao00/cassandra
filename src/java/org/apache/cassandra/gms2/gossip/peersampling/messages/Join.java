package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class Join implements HyParViewMessage
{
    private static final ISerializer<Join> serializer = new Serializer();

    public MessageType getMessageType()
    {
        return MessageType.JOIN;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    public static class Serializer implements ISerializer<Join>
    {
        public Join deserialize(DataInput in) throws IOException
        {
            return new Join();
        }

        public void serialize(Join msg, DataOutputPlus out) throws IOException
        {
            // nop
        }

        public long serializedSize(Join msg, TypeSizes type)
        {
            return 0;
        }
    }
}
