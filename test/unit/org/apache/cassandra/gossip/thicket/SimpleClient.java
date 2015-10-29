package org.apache.cassandra.gossip.thicket;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gossip.BroadcastServiceClient;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

class SimpleClient implements BroadcastServiceClient<String>
{
    private final List<String> received = new LinkedList<>();

    public String getClientName()
    {
        return "simple-client";
    }

    public boolean receive(String payload)
    {
        if (received.contains(payload))
            return false;
        received.add(payload.toString());
        return true;
    }

    public IVersionedSerializer<String> getSerializer()
    {
        return new IVersionedSerializer<String>()
        {
            public void serialize(String s, DataOutputPlus out, int version) throws IOException
            {
                out.writeUTF(s);
            }

            public long serializedSize(String s, int version)
            {
                return TypeSizes.sizeof(s);
            }

            public String deserialize(DataInputPlus in, int version) throws IOException
            {
                return in.readUTF();
            }
        };
    }
}
