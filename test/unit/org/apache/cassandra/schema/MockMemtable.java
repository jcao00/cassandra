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

package org.apache.cassandra.schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.MemtableFactory;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class MockMemtable implements Memtable
{
    private final ColumnsCollector columnsCollector;
    private final ClusteringComparator initialComparator;

    public MockMemtable(TableMetadata metadata)
    {
        this.initialComparator = metadata.comparator;
        this.columnsCollector = new ColumnsCollector(metadata.regularAndStaticColumns());
    }

    @Override
    public ColumnFamilyStore cfs()
    {
        return null;
    }

    @Override
    public long getLiveDataSize()
    {
        return 0;
    }

    @Override
    public long getOperations()
    {
        return 0;
    }

    @Override
    public void setDiscarding(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {

    }

    @Override
    public void setDiscarded()
    {

    }

    @Override
    public CommitLogPosition getCommitLogLowerBound()
    {
        return CommitLogPosition.NONE;
    }

    @Override
    public CommitLogPosition getCommitLogUpperBound()
    {
        return CommitLogPosition.NONE;
    }

    @Override
    public ClusteringComparator getInitialComparator()
    {
        return initialComparator;
    }

    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        return true;
    }

    @Override
    public boolean isLive()
    {
        return false;
    }

    @Override
    public boolean isClean()
    {
        return false;
    }

    @Override
    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return false;
    }

    @Override
    public boolean isExpired()
    {
        return false;
    }

    @Override
    public void adjustMemtableSize(Region region, long additionalSpace, OpOrder.Group opGroup)
    {

    }

    @Override
    public float getOwnershipRatio(Region region)
    {
        return 0;
    }

    @Override
    public long getOwns(Region region)
    {
        return 0;
    }

    @Override
    public float getUsedRatio(Region region)
    {
        return 0;
    }

    @Override
    public float getReclaimingRatio(Region region)
    {
        return 0;
    }

    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        return 0;
    }

    @Override
    public int partitionCount()
    {
        return 0;
    }

    @Override
    public List<? extends Callable<SSTableMultiWriter>> flushRunnables(LifecycleTransaction txn)
    {
        return Collections.emptyList();
    }

    @Override
    public Throwable abortRunnables(List<? extends Callable<SSTableMultiWriter>> runnables, Throwable t)
    {
        return null;
    }

    @Override
    public MemtableUnfilteredPartitionIterator makePartitionIterator(ColumnFilter columnFilter, DataRange dataRange)
    {
        return null;
    }

    @Override
    public Partition getPartition(DecoratedKey key)
    {
        return null;
    }

    @Override
    public long getMinTimestamp()
    {
        return 0;
    }

    @Override
    public void makeUnflushable()
    {

    }

    @Override
    public int compareTo(Memtable o)
    {
        return 0;
    }
}
