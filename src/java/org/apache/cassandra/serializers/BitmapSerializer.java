package org.apache.cassandra.serializers;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.apple.aml.bitmap.RoaringBitmap;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;

public class BitmapSerializer implements TypeSerializer<RoaringBitmap>
{
    public static final BitmapSerializer instance = new BitmapSerializer();

    public ByteBuffer serialize(RoaringBitmap value)
    {
        DataOutputBuffer in = new DataOutputBuffer(value.serializedSize());
        try
        {
            value.serialize(in);
            return ByteBuffer.wrap(in.getData());
        }
        catch (IOException e)
        {
            throw new RuntimeException("failed to serialize roaring bitmap to buffer");
        }
    }

    public RoaringBitmap deserialize(ByteBuffer bytes)
    {
        DataInput in = new DataInputStream(new FastByteArrayInputStream(bytes.array()));
        try
        {
            return RoaringBitmap.deserialize(in);
        }
        catch (IOException e)
        {
            throw new RuntimeException("failed to deserialize buffer to roaring bitmap");
        }
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        //TODO:JEB impl me
    }

    public String toString(RoaringBitmap value)
    {
        return null;
    }

    public Class<RoaringBitmap> getType()
    {
        return RoaringBitmap.class;
    }
}
