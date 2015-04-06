package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ShuffleResponse implements HyParViewMessage
{
    private static final ISerializer<ShuffleResponse> serializer = new Serializer();

    private final Collection<InetSocketAddress> nodes;
    private final Collection<InetSocketAddress> sentNodes;

    public ShuffleResponse(Collection<InetSocketAddress> peers, Collection<InetSocketAddress> sentNodes)
    {
        this.nodes = peers;
        this.sentNodes = sentNodes;
    }

    public Collection<InetSocketAddress> getNodes()
    {
        return nodes;
    }

    public Collection<InetSocketAddress> getSentNodes()
    {
        return sentNodes;
    }

    public MessageType getMessageType()
    {
        return MessageType.SHUFFLE_RESPONSE;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<ShuffleResponse>
    {
        public ShuffleResponse deserialize(DataInput in) throws IOException
        {
            return null;
        }

        public void serialize(ShuffleResponse msg, DataOutputPlus out) throws IOException
        {

        }

        public long serializedSize(ShuffleResponse msg, TypeSizes type)
        {
            return 0;
        }
    }
}
