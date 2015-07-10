package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import com.apple.aml.bitmap.RoaringBitmap;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.BitmapSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;

public class BitmapType extends AbstractType<RoaringBitmap>
{
    public static final BitmapType instance = new BitmapType();

    BitmapType() { }

    public boolean isCommutative()
    {
        return true;
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        return LongType.instance.fromString(source);
    }

    public TypeSerializer<RoaringBitmap> getSerializer()
    {
        return BitmapSerializer.instance;
    }

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return 0;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BITMAP;
    }
}
