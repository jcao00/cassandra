package org.apache.cassandra.streaming.messages;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamSession;

public class CanceledStreamMessage extends StreamMessage
{
    public CanceledStreamMessage()
    {
        super(Type.CANCELED);
    }

    public static Serializer<CanceledStreamMessage> serializer = new Serializer<CanceledStreamMessage>()
    {
        public CanceledStreamMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            return new CanceledStreamMessage();
        }

        public void serialize(CanceledStreamMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException {}
    };

}
