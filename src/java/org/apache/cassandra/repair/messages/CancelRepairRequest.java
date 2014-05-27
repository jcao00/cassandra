package org.apache.cassandra.repair.messages;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.utils.UUIDSerializer;

public class CancelRepairRequest extends RepairMessage
{
    public static MessageSerializer serializer = new CancelRepairSerializer();
    public final UUID parentRepairSession;

    public CancelRepairRequest(UUID parentRepairSession)
    {
        super(Type.CANCEL_REPAIR, null);
        this.parentRepairSession = parentRepairSession;
    }

    public MessageOut<RepairMessage> createMessage()
    {
        return super.createMessage();
    }

    public static class CancelRepairSerializer implements MessageSerializer<CancelRepairRequest>
    {
        public void serialize(CancelRepairRequest message, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out, version);
        }

        public CancelRepairRequest deserialize(DataInput in, int version) throws IOException
        {
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in, version);
            return new CancelRepairRequest(parentRepairSession);
        }

        public long serializedSize(CancelRepairRequest message, int version)
        {
            return UUIDSerializer.serializer.serializedSize(message.parentRepairSession, version);
        }
    }
}
