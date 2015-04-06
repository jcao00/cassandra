package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class JoinAck implements HyParViewMessage
{
    private static final ISerializer<JoinAck> serializer = new Serializer();

    public MessageType getMessageType()
    {
        return MessageType.JOIN_ACK;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<JoinAck>
    {
        public JoinAck deserialize(DataInput in) throws IOException
        {
            return new JoinAck();
        }

        public void serialize(JoinAck msg, DataOutputPlus out) throws IOException
        {
            // nop
        }

        public long serializedSize(JoinAck msg, TypeSizes type)
        {
            return 0;
        }
    }
}
