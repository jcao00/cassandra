package org.apache.cassandra.gms2.gossip.peersampling.messages;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ForwardJoin implements HyParViewMessage
{
    private static final ISerializer<ForwardJoin> serializer = new Serializer();

    public final InetSocketAddress originator;
    public final byte timeToLive;
    public final byte activeRandomWalkLength;
    public final byte passiveRandomWalkLength;

    public ForwardJoin(InetSocketAddress originator, byte timeToLive, byte activeRandomWalkLength, byte passiveRandomWalkLength)
    {
        this.originator = originator;
        this.timeToLive = timeToLive;
        this.activeRandomWalkLength = activeRandomWalkLength;
        this.passiveRandomWalkLength = passiveRandomWalkLength;
    }

    /**
     * Shallow clone and decrement the {@code timeToLive} value.
     */
    public ForwardJoin cloneForForwarding()
    {
        return new ForwardJoin(originator, (byte)(timeToLive - 1), activeRandomWalkLength, passiveRandomWalkLength);
    }

    public MessageType getMessageType()
    {
        return MessageType.FORWARD_JOIN;
    }

    public ISerializer getSerializer()
    {
        return serializer;
    }

    private static final class Serializer implements ISerializer<ForwardJoin>
    {
        public ForwardJoin deserialize(DataInput in) throws IOException
        {
            return new ForwardJoin(Utils.deserialize(in), in.readByte(), in.readByte(), in.readByte());
        }

        public void serialize(ForwardJoin msg, DataOutputPlus out) throws IOException
        {
            Utils.serialize(msg.originator, out);
            out.writeByte(msg.timeToLive);
            out.writeByte(msg.activeRandomWalkLength);
            out.writeByte(msg.passiveRandomWalkLength);
        }

        public long serializedSize(ForwardJoin msg, TypeSizes type)
        {
            return 0;
        }
//        {
//            return 1 + forwardJoin.originator.getAddress().getAddress().length + 2  // addr len +  ipAddr bytes + 2 (for port)
//                    + 3; // (ttl, active len, passive len)
//        }
    }
}
