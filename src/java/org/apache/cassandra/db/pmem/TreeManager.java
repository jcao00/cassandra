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

package org.apache.cassandra.db.pmem;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.google.common.util.concurrent.Futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lib.llpl.Heap;
import lib.util.persistent.ObjectDirectory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.jctools.maps.NonBlockingHashMap;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.QueueFactory;
import org.jctools.queues.spec.ConcurrentQueueSpec;

// TODO:JEB this is really more of a dispatcher
public class TreeManager
{
    private static final Logger logger = LoggerFactory.getLogger(TreeManager.class);

    private static final int CURRENT_VERSION = 1;

    // JEB: this has to be set *once*, on first launch, as we'll use that to determine token sharding across cores,
    // in perpituity for this instance.
    private static final int CORES = FBUtilities.getAvailableProcessors();

    private static final Thread[] threads;
    private static final MessagePassingQueue<FutureTask<?>>[] queues;

    private static final NonBlockingHashMap<TableId, Tree[]> treesMap;

    // TODO:JEB need to have a map of heaps, one entry per each dimm/namespace/etc ....
    static final Heap heap;

    static
    {
        ConcurrentQueueSpec boundedMpsc = ConcurrentQueueSpec.createBoundedMpsc(1024);
        threads = new Thread[CORES];
        queues = new MessagePassingQueue[CORES];
        treesMap = new NonBlockingHashMap<>();
        for (int i = 0; i < threads.length; i++)
        {
            final MessagePassingQueue queue = (MessagePassingQueue)QueueFactory.newQueue(boundedMpsc);
            queues[i] = queue;
            threads[i] = new Thread(() -> execute(queue), "thread-tree-" + i);
        }

        heap = Heap.getHeap();
        if (heap.getRoot() == 0)
        {
            // TODO: if there's no root set, then this is a brand new heap. thus we need to give it a new root.
            // that root should be the "base address" of the map in which we'll store references to all the trees.
//            heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, )
//            heap.setRoot();
        }
    }

    private static volatile boolean shutdown;

    /**
     * THIS MUST BE INVOKED IMMEDIATELY DURING/RIGHT AFTER CREATING/INSTANTIATING A CFS,
     * else everything goes to awful-ville.
     *
     * This method is synchronized because I'm too n00b to trust how I open/create the pmem files.
     * This won't be called frequently, at all, and doesn't need to be high perf (as we're opening
     * files), so screw it.
     * @param tableId
     */
    public TreeManager(TableId tableId)
    {
        if (treesMap.contains(tableId))
            return;

        // for system tables, I think I have to do this before we actaully confirm the tokens are legitimately ours.
        // perhaps just assign all tables in system keyspace to shard 0 and call it a day?
        // force system-wide keyspaces to be have a single tree, as we don't know token distribution immediately
        // at boot time.
        // TODO:JEB figure out what to do about system tables - only allowing keyspace 'pmem' right now, so it doesn't matter
        int treeCount = CORES;
        Tree[] trees = new Tree[treeCount];
        for (int i = 0; i < treeCount; i++)
        {
            // all treees are combined into one big file in the pool.
            // this is because the file metadata is assigned at file creation, and we can't predict the size of each
            // potential tree relative to every one at creation time. hence, one big file (pool) with many trees.

            String key = toKey(tableId, i);

            // TODO:JEB replace ObjectDiretory with something like a PersistentUnsafeHashMap, that does basically the same thing.
            // NOTE: PersistentUnsafeHashMap will need to be safe, or at least, synchronized everywhere -
            // which is fine for it's use case
            Tree tree = ObjectDirectory.get(key, Tree.class);
            if (tree == null)
            {
                tree = new Tree(heap);
                ObjectDirectory.put(key, tree);
            }

            trees[i] = tree;
        }

        treesMap.putIfAbsent(tableId, trees);
    }

    private static String toKey(TableId tableId, int shard)
    {
        return String.format("%s-%d-%d", tableId.toString(), shard, CURRENT_VERSION);
    }

    /*
        public functions to operate on data
    */

    public Future<Void> apply(ColumnFamilyStore cfs, final PartitionUpdate update, final UpdateTransaction indexer, final OpOrder.Group opGroup)
    {
        if (shutdown)
            return Futures.immediateCancelledFuture();

        final int index = findTreeIndex(update.partitionKey());
        final FutureTask<Void> future = new FutureTask<>(new WriteOperation(getTree(cfs.metadata.id, index), update, indexer, opGroup));

        // this might return false for a bounded queue that is filled up, so ????
        if (queues[index].offer(future))
        {
            logger.info("couldn't add write to queue");
            // TODO:JEB fail the future
        }

        return future;
    }

    private int findTreeIndex(DecoratedKey key)
    {
        // JEB: this is weak ... but good enough for now. don't want to build token range checking atm ....
        Long l = (Long)key.getToken().getTokenValue();
        return (int)(l % CORES);
    }

    private Tree getTree(TableId tableId, int index)
    {
        Tree[] trees = treesMap.get(tableId);

        if (trees != null)
            return trees[index];
        throw new IllegalStateException("attempted to get a tree for a CFS that has not been initialized yet; table Id = " + tableId);
    }

    // uuummm , i need more than just the DK, right? like how about clusterings, etc .... look at SPRC
    public Future<UnfilteredRowIterator> select(ColumnFamilyStore cfs, DecoratedKey decoratedKey)
    {
        if (shutdown)
            return Futures.immediateCancelledFuture();

        final int index = findTreeIndex(decoratedKey);
        final FutureTask<UnfilteredRowIterator> future = new FutureTask<>(new ReadOperation(getTree(cfs.metadata.id, index), decoratedKey));

        // this might return false for a bounded queue that is filled up, so ????
        if (queues[index].offer(future))
        {
            logger.info("couldn't add query to queue");
            // TODO:JEB fail the future
        }

        return future;
    }

    /*
        async tree functions
     */

    private void handleWrite(Tree tree, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        // basically ignore the indexer and opGroup ... for now
        PmemTransaction tx = new PmemTransaction();
        // TODO: probably need a try/catch block here ...
        tree.apply(update, tx);
    }

    private UnfilteredRowIterator handleRead(Tree tree, DecoratedKey decoratedKey)
    {
        // TODO: wrap the response in a UnfilteredRowIterator, somehows...
        return tree.get(decoratedKey);
    }

    /*
        thread / queue functions
     */

    public static void execute(MessagePassingQueue<FutureTask> queue)
    {
        queue.drain(FutureTask::run, idleCounter -> idleCounter, () -> !shutdown);
    }

    class WriteOperation implements Callable<Void>
    {
        private final Tree tree;
        private final PartitionUpdate update;
        private final UpdateTransaction indexer;
        private final OpOrder.Group opGroup;

        WriteOperation(Tree tree, PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        {
            this.tree = tree;
            this.update = update;
            this.indexer = indexer;
            this.opGroup = opGroup;
        }

        @Override
        public Void call()
        {
            handleWrite(tree, update, indexer, opGroup);
            return null;
        }
    }

    class ReadOperation implements Callable<UnfilteredRowIterator>
    {
        private final Tree tree;
        private final DecoratedKey decoratedKey;

        ReadOperation(Tree tree, DecoratedKey decoratedKey)
        {
            this.tree = tree;
            this.decoratedKey = decoratedKey;
        }

        @Override
        public UnfilteredRowIterator call()
        {
            return handleRead(tree, decoratedKey);
        }
    }

    public void shutdown()
    {
        shutdown = true;
    }
}
