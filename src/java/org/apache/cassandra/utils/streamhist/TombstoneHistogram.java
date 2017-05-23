/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.utils.streamhist;

import java.io.IOException;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder.DataHolder;


public class TombstoneHistogram
{
    public static final HistogramSerializer serializer = new HistogramSerializer();
    public static final AsOf4_0Serializer fourDotZeroSerializer = new AsOf4_0Serializer();
    public static final LegacyHistogramSerializer legacySerializer = new LegacyHistogramSerializer();

    // Buffer with point-value pair
    private final DataHolder bin;

    // maximum bin size for this histogram
    private final int maxBinSize;

    TombstoneHistogram(int maxBinSize, DataHolder holder)
    {
        this.maxBinSize = maxBinSize;
        bin = new DataHolder(holder);
    }

    public static TombstoneHistogram createDefault()
    {
        int maxBinSize = SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE;
        return new TombstoneHistogram(maxBinSize, new DataHolder(maxBinSize, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS));
    }

    /**
     * Calculates estimated number of points in interval [-inf,b].
     *
     * @param b upper bound of a interval to calculate sum
     * @return estimated number of points in a interval [-inf,b].
     */
    public double sum(double b)
    {
        return bin.sum((int) b);
    }

    public int size()
    {
        int[] acc = new int[1];
        this.bin.forEach((point, value) -> acc[0]++);
        return acc[0];
    }

    public <E extends Exception> void forEach(PointAndValueConsumer<E> pointAndValueConsumer) throws E
    {
        this.bin.forEach(pointAndValueConsumer);
    }

    public static class HistogramSerializer
    {
        public int serializedSize(Version version, TombstoneHistogram histogram) throws IOException
        {
            return version.hasOptimizedStreamingHistogramSerialization() ?
                   (int) fourDotZeroSerializer.serializedSize(histogram) :
                   (int) legacySerializer.serializedSize(histogram);
        }

        public void serialize(Version version, TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            if (version.hasOptimizedStreamingHistogramSerialization())
                fourDotZeroSerializer.serialize(histogram, out);
            else
                legacySerializer.serialize(histogram, out);
        }

        public TombstoneHistogram deserialize(Version version, DataInputPlus in) throws IOException
        {
            return version.hasOptimizedStreamingHistogramSerialization() ?
                   fourDotZeroSerializer.deserialize(in) :
                   legacySerializer.deserialize(in);
        }
    }

    private static class AsOf4_0Serializer implements ISerializer<TombstoneHistogram>
    {
        public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            out.writeInt(histogram.maxBinSize);
            out.writeInt(histogram.size());
            histogram.forEach((point, value) ->
                              {
                                  out.writeInt(point);
                                  out.writeInt(value);
                              });
        }

        public TombstoneHistogram deserialize(DataInputPlus in) throws IOException
        {
            int maxBinSize = in.readInt();
            int size = in.readInt();

            DataHolder bin = new DataHolder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
            for (int i = 0; i < size; i++)
                bin.addValue(in.readInt(), in.readInt());

            return new TombstoneHistogram(maxBinSize, bin);
        }

        public long serializedSize(TombstoneHistogram histogram)
        {
            long size = TypeSizes.sizeof(histogram.maxBinSize);
            final int histSize = histogram.size();
            size += TypeSizes.sizeof(histSize);
            // size of entries = size * (4(int) + 4(int))
            size += histSize * (4L + 4L);
            return size;
        }
    }

    private static class LegacyHistogramSerializer implements ISerializer<TombstoneHistogram>
    {
        public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            out.writeInt(histogram.maxBinSize);
            out.writeInt(histogram.size());
            histogram.forEach((point, value) ->
                              {
                                  out.writeDouble((double) point);
                                  out.writeLong((long) value);
                              });
        }

        public TombstoneHistogram deserialize(DataInputPlus in) throws IOException
        {
            int maxBinSize = in.readInt();
            int size = in.readInt();

            DataHolder bin = new DataHolder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
            for (int i = 0; i < size; i++)
                bin.addValue((int)in.readDouble(), (int)in.readLong());

            return new TombstoneHistogram(maxBinSize, bin);
        }

        public long serializedSize(TombstoneHistogram histogram)
        {
            long size = TypeSizes.sizeof(histogram.maxBinSize);
            final int histSize = histogram.size();
            size += TypeSizes.sizeof(histSize);
            // size of entries = size * (8(double) + 8(long))
            size += histSize * (8L + 8L);
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TombstoneHistogram))
            return false;

        TombstoneHistogram that = (TombstoneHistogram) o;
        return maxBinSize == that.maxBinSize &&
               bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bin.hashCode(), maxBinSize);
    }
}
