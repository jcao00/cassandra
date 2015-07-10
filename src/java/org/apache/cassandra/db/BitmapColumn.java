package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.apple.aml.bitmap.RoaringBitmap;
import org.apache.cassandra.serializers.BitmapSerializer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;

public class BitmapColumn extends Column
{
    private final RoaringBitmap roaringBitmap;

    public BitmapColumn(ByteBuffer name, long l)
    {
        super(name);
        roaringBitmap = new RoaringBitmap();
        roaringBitmap.add(l);
    }

    public BitmapColumn(ByteBuffer name, DataInput in, long timestamp)
    {
        super(name, EMPTY_BYTE_BUFFER, timestamp);
        try
        {
            roaringBitmap = RoaringBitmap.deserialize(in);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    BitmapColumn(ByteBuffer name, RoaringBitmap bitmap, long timestamp)
    {
        //TODO:JEB this is prolly wrong...
        super(name, EMPTY_BYTE_BUFFER, timestamp);
        roaringBitmap = bitmap;
    }

    public ByteBuffer value()
    {
        return BitmapSerializer.instance.serialize(roaringBitmap);
    }

    public Column withUpdatedName(ByteBuffer newName)
    {
        return new BitmapColumn(newName, roaringBitmap, timestamp());
    }

    protected int getValueSerializationSize()
    {
        return roaringBitmap.serializedSize();
    }

    public Column diff(Column column)
    {
        if (column.getClass() != this.getClass())
            throw new IllegalArgumentException();

        BitmapColumn other = (BitmapColumn)column;
        RoaringBitmap diff = RoaringBitmap.xor(roaringBitmap, other.roaringBitmap);

        return diff.getCardinality() == 0 ? null : new BitmapColumn(name(), diff, timestamp());
    }

    public void updateDigest(MessageDigest digest)
    {
        // TODO:JEB do something here ....
    }

    public Column reconcile(Column column, Allocator allocator)
    {
        if (column.getClass() != this.getClass())
            throw new IllegalArgumentException();

        roaringBitmap.or(((BitmapColumn)column).roaringBitmap);
        return this;
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof BitmapColumn))
            return false;
        BitmapColumn other = (BitmapColumn)o;
        if (timestamp != other.timestamp)
            return false;
        if (!name.equals(other.name))
            return false;

        return RoaringBitmap.xor(roaringBitmap, other.roaringBitmap).getCardinality() == 0;
    }

    public Column localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new BitmapColumn(name(), roaringBitmap, timestamp());
    }

    public int serializationFlags()
    {
        return ColumnSerializer.BITMAP_MASK;
    }

    public void serialize(DataOutput out) throws IOException
    {
        roaringBitmap.serialize(out);
    }
}
