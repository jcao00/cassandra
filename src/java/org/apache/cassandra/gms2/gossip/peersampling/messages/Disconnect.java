package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class Disconnect implements HyParViewMessage
{
    private static final ISerializer<Disconnect> serializer = new Serializer();

    public MessageType getMessageType()
    {
        return MessageType.DISCONNECT;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<Disconnect>
    {
        public Disconnect deserialize(DataInput in) throws IOException
        {
            return new Disconnect();
        }

        public void serialize(Disconnect msg, DataOutputPlus out) throws IOException
        {
            // nop
        }

        public long serializedSize(Disconnect msg, TypeSizes type)
        {
            return 0;
        }
    }
}
