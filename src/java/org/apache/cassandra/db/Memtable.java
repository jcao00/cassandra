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
package org.apache.cassandra.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public abstract class Memtable implements Comparable<Memtable>
{
//    private static final Logger logger = LoggerFactory.getLogger(Memtable.class);
//
//    private final AtomicLong liveDataSize = new AtomicLong(0);
//    private final AtomicLong currentOperations = new AtomicLong(0);
//
//    // the write barrier for directing writes to this memtable during a switch
//    private volatile OpOrder.Barrier writeBarrier;
//    // the precise upper bound of CommitLogPosition owned by this memtable
//    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;
//    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
//    private AtomicReference<CommitLogPosition> commitLogLowerBound;
//
//    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
//    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
//    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();
//
//    public int compareTo(Memtable that)
//    {
//        return this.approximateCommitLogLowerBound.compareTo(that.approximateCommitLogLowerBound);
//    }
//

    // TODO:JEB this is really not related to memtable ... should be moved elsewhere.
    // we only care about the type in StdMemtable#accepts()
    // ata a minimum, the name is non-descriptive
    public static final class LastCommitLogPosition extends CommitLogPosition
    {
        public LastCommitLogPosition(CommitLogPosition copy)
        {
            super(copy.segmentId, copy.position);
        }
    }
//
//    // We index the memtable by PartitionPosition only for the purpose of being able
//    // to select key range using Token.KeyBound. However put() ensures that we
//    // actually only store DecoratedKey.
//    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();
    public final ColumnFamilyStore cfs;
    private final long creationNano = System.nanoTime();
//
//    // The smallest timestamp for all partitions stored in this memtable
//    private long minTimestamp = Long.MAX_VALUE;
//
    // Record the comparator of the CFS at the creation of the memtable. This
    // is only used when a user update the CF comparator, to know if the
    // memtable was created with the new or old comparator.
    public final ClusteringComparator initialComparator;
//
    final ColumnsCollector columnsCollector;
//    private final StatsCollector statsCollector = new StatsCollector();
//
    // only to be used by init(), to setup the very first memtable for the cfs
    Memtable(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
//        this.commitLogLowerBound = commitLogLowerBound;
        this.initialComparator = cfs.metadata().comparator;
        this.cfs.scheduleFlush();
        this.columnsCollector = new ColumnsCollector(cfs.metadata().regularAndStaticColumns());
    }
//
//    // ONLY to be used for testing, to create a mock Memtable
//    @VisibleForTesting
//    public Memtable(TableMetadata metadata)
//    {
//        this.initialComparator = metadata.comparator;
//        this.cfs = null;
//        this.allocator = null;
//        this.columnsCollector = new ColumnsCollector(metadata.regularAndStaticColumns());
//    }
//
    public abstract MemtableAllocator getAllocator();
//
    public abstract long getLiveDataSize();

    public abstract long getOperations();
//
    @VisibleForTesting
    public abstract void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound);

    abstract void  setDiscarded();

    public abstract CommitLogPosition getCommitLogLowerBound();
    public abstract CommitLogPosition getCommitLogUpperBound();

//
//    // decide if this memtable should take the write, or if it should go to the next memtable
    public abstract  boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition);

    //    public CommitLogPosition getCommitLogLowerBound()
//    {
//        return commitLogLowerBound.get();
//    }
//
//    public CommitLogPosition getCommitLogUpperBound()
//    {
//        return commitLogUpperBound.get();
//    }
//
    public abstract boolean isLive();

    public abstract boolean isClean();
//
    public abstract boolean mayContainDataBefore(CommitLogPosition position);
//    {
//        return approximateCommitLogLowerBound.compareTo(position) < 0;
//    }
//
//    /**
//     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
//     */
    public abstract boolean isExpired();
//
//    /**
//     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
//     * OpOrdering.
//     *
//     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
//     */
    abstract long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup);
//
    public abstract int partitionCount();

    public abstract List<? extends Callable<SSTableMultiWriter>> flushRunnables(LifecycleTransaction txn);


    public abstract Throwable abortRunnables(List<? extends Callable<SSTableMultiWriter>> runnables, Throwable t);

    public abstract MemtableUnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange);

    public abstract Partition getPartition(DecoratedKey key);

    public abstract long getMinTimestamp();

    public abstract AllocationStats getCurrentAllocationStats();

    //    /**
//     * For testing only. Give this memtable too big a size to make it always fail flushing.
//     */
//    @VisibleForTesting
//    public void makeUnflushable()
//    {
//        liveDataSize.addAndGet(1L * 1024 * 1024 * 1024 * 1024 * 1024);
//    }
//

//    private static int estimateRowOverhead(final int count)
//    {
//        // calculate row overhead
//        try (final OpOrder.Group group = new OpOrder().start())
//        {
//            int rowOverhead;
//            MemtableAllocator allocator = MEMORY_POOL.newAllocator();
//            ConcurrentNavigableMap<PartitionPosition, Object> partitions = new ConcurrentSkipListMap<>();
//            final Object val = new Object();
//            for (int i = 0 ; i < count ; i++)
//                partitions.put(allocator.clone(new BufferDecoratedKey(new LongToken(i), ByteBufferUtil.EMPTY_BYTE_BUFFER), group), val);
//            double avgSize = ObjectSizes.measureDeep(partitions) / (double) count;
//            rowOverhead = (int) ((avgSize - Math.floor(avgSize)) < 0.05 ? Math.floor(avgSize) : Math.ceil(avgSize));
//            rowOverhead -= ObjectSizes.measureDeep(new LongToken(0));
//            rowOverhead += AtomicBTreePartition.EMPTY_SIZE;
//            allocator.setDiscarding();
//            allocator.setDiscarded();
//            return rowOverhead;
//        }
//    }
//

    public static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator
    {
        private final ColumnFamilyStore cfs;
        private final Iterator<Map.Entry<PartitionPosition, Partition>> iter;
        private final int minLocalDeletionTime;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(ColumnFamilyStore cfs, Iterator<Map.Entry<PartitionPosition, Partition>> iter, int minLocalDeletionTime, ColumnFilter columnFilter, DataRange dataRange)
        {
            this.cfs = cfs;
            this.iter = iter;
            this.minLocalDeletionTime = minLocalDeletionTime;
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public int getMinLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }

        public TableMetadata metadata()
        {
            return cfs.metadata();
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Map.Entry<PartitionPosition, ? extends Partition> entry = iter.next();
            // Actual stored key should be true DecoratedKey
            assert entry.getKey() instanceof DecoratedKey;
            DecoratedKey key = (DecoratedKey)entry.getKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, entry.getValue());
        }
    }

    static class ColumnsCollector
    {
        private final HashMap<ColumnMetadata, AtomicBoolean> predefined = new HashMap<>();
        private final ConcurrentSkipListSet<ColumnMetadata> extra = new ConcurrentSkipListSet<>();
        ColumnsCollector(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata def : columns.statics)
                predefined.put(def, new AtomicBoolean());
            for (ColumnMetadata def : columns.regulars)
                predefined.put(def, new AtomicBoolean());
        }

        public void update(RegularAndStaticColumns columns)
        {
            for (ColumnMetadata s : columns.statics)
                update(s);
            for (ColumnMetadata r : columns.regulars)
                update(r);
        }

        private void update(ColumnMetadata definition)
        {
            AtomicBoolean present = predefined.get(definition);
            if (present != null)
            {
                if (!present.get())
                    present.set(true);
            }
            else
            {
                extra.add(definition);
            }
        }

        public RegularAndStaticColumns get()
        {
            RegularAndStaticColumns.Builder builder = RegularAndStaticColumns.builder();
            for (Map.Entry<ColumnMetadata, AtomicBoolean> e : predefined.entrySet())
                if (e.getValue().get())
                    builder.add(e.getKey());
            return builder.addAll(extra).build();
        }
    }
//
//    private static class StatsCollector
//    {
//        private final AtomicReference<EncodingStats> stats = new AtomicReference<>(EncodingStats.NO_STATS);
//
//        public void update(EncodingStats newStats)
//        {
//            while (true)
//            {
//                EncodingStats current = stats.get();
//                EncodingStats updated = current.mergeWith(newStats);
//                if (stats.compareAndSet(current, updated))
//                    return;
//            }
//        }
//
//        public EncodingStats get()
//        {
//            return stats.get();
//        }
//    }

    public static class AllocationStats
    {
        public final float onHeapUsedRatio;
        public final float offHeapUsedRatio;
        public final float onHeapReclaimingRatio;
        public final float offHeapReclaimingRatio;

        AllocationStats(float onHeapUsedRatio, float offHeapUsedRatio, float onHeapReclaimingRatio, float offHeapReclaimingRatio)
        {
            this.onHeapUsedRatio = onHeapUsedRatio;
            this.offHeapUsedRatio = offHeapUsedRatio;
            this.onHeapReclaimingRatio = onHeapReclaimingRatio;
            this.offHeapReclaimingRatio = offHeapReclaimingRatio;
        }
    }
}
