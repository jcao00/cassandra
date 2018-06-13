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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapPool;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtablePool;
import org.apache.cassandra.utils.memory.NativePool;
import org.apache.cassandra.utils.memory.SlabPool;

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
//    public static final class LastCommitLogPosition extends CommitLogPosition
//    {
//        public LastCommitLogPosition(CommitLogPosition copy)
//        {
//            super(copy.segmentId, copy.position);
//        }
//    }
//
//    // We index the memtable by PartitionPosition only for the purpose of being able
//    // to select key range using Token.KeyBound. However put() ensures that we
//    // actually only store DecoratedKey.
//    private final ConcurrentNavigableMap<PartitionPosition, AtomicBTreePartition> partitions = new ConcurrentSkipListMap<>();
//    public final ColumnFamilyStore cfs;
//    private final long creationNano = System.nanoTime();
//
//    // The smallest timestamp for all partitions stored in this memtable
//    private long minTimestamp = Long.MAX_VALUE;
//
//    // Record the comparator of the CFS at the creation of the memtable. This
//    // is only used when a user update the CF comparator, to know if the
//    // memtable was created with the new or old comparator.
//    public final ClusteringComparator initialComparator;
//
//    private final ColumnsCollector columnsCollector;
//    private final StatsCollector statsCollector = new StatsCollector();
//
//    // only to be used by init(), to setup the very first memtable for the cfs
//    public Memtable(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs)
//    {
//        this.cfs = cfs;
//        this.commitLogLowerBound = commitLogLowerBound;
//        this.allocator = MEMORY_POOL.newAllocator();
//        this.initialComparator = cfs.metadata().comparator;
//        this.cfs.scheduleFlush();
//        this.columnsCollector = new ColumnsCollector(cfs.metadata().regularAndStaticColumns());
//    }
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
//    public boolean mayContainDataBefore(CommitLogPosition position)
//    {
//        return approximateCommitLogLowerBound.compareTo(position) < 0;
//    }
//
//    /**
//     * @return true if this memtable is expired. Expiration time is determined by CF's memtable_flush_period_in_ms.
//     */
//    public boolean isExpired()
//    {
//        int period = cfs.metadata().params.memtableFlushPeriodInMs;
//        return period > 0 && (System.nanoTime() - creationNano >= TimeUnit.MILLISECONDS.toNanos(period));
//    }
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

    public abstract List<Callable<SSTableMultiWriter>> flushRunnables(LifecycleTransaction txn);


    public abstract Throwable abortRunnables(List<Callable<SSTableMultiWriter>> runnables, Throwable t);

    public abstract UnfilteredPartitionIterator makePartitionIterator(final ColumnFilter columnFilter, final DataRange dataRange);

    public abstract Partition getPartition(DecoratedKey key);

    public abstract long getMinTimestamp();

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
}
