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

import java.nio.ByteBuffer;

import lib.llpl.Heap;
import lib.llpl.MemoryRegion;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;

// key type = DectoratedKey
// value type = PMemParition
public class Tree
{
    private static final int MAX_INTERNAL_KEYS = 8;
    private static final int MAX_LEAF_KEYS = 64;

    /**
     * HEAD_LEAF is a fixed sentinal for this tree instance. It is critical for restarting the process, as it allows us
     * to have a known starting address from which to reconstruct the tree.
     */
    private final PersistentLeaf HEAD_LEAF;
    private final Heap heap;

    private Node root;

    public Tree(Heap heap, PmemTransaction tx)
    {
        this.heap = heap;
        root = new LeafNode(heap, tx);
    }

    // hmm, I guess this ctor is the "reconstruct entire tree" entry point, as we have the addr for
    // the first persistent leaf ...
    public Tree(Heap heap, long address)
    {
        this.heap = heap;
        root = new LeafNode(heap, address);
    }

    public void apply(PartitionUpdate update, PmemTransaction tx)
    {
        DecoratedKey decoratedKey = update.partitionKey();
        long token = ((LongToken) (decoratedKey.getToken())).getLongValue();
        insert(decoratedKey, token, update, tx);
    }

    public PMemPartition get(DecoratedKey key)
    {
        long token = ((LongToken) (key.getToken())).getLongValue();
        return null;
    }





    private static class Node
    {
        // save space in the tree by using fixed constants to represent the color, instead of a enum instance/object
        private static final boolean COLOR_RED = true;
        private static final boolean COLOR_BLUE = false;

        boolean color;
    }

    private static class InnerNode extends Node
    {
        // there might be more compact representations than a full 8-byte long for the DK tokens, but this is already better
        // than needing to the DK compares for the whole tree.
        final long[] tokens;
        final Node[] children;
        int size;

        private InnerNode()
        {
            tokens = new long[MAX_INTERNAL_KEYS];
            children = new Node[MAX_INTERNAL_KEYS + 1]; /// JEB: double check this is correct
        }
    }

    // all leaves are BLACK in an RBtree
    private static class LeafNode extends Node
    {
        private final PersistentLeaf persistentLeaf;
        private LeafNode previous;
        private LeafNode next;

        private LeafNode(Heap heap, PmemTransaction tx)
        {
            persistentLeaf = PersistentLeaf.create(heap, tx);
        }

        private LeafNode(Heap heap, long address)
        {
            persistentLeaf = PersistentLeaf.load(heap, address);
        }
    }

    // this is wheere the rubber meets the road wrt actual persisted data in nvram
    private static class PersistentLeaf
    {
        private static final int TOKENS_OFFSET = 0;
        private static final int PARTITION_ADDRESSES_OFFSET = MAX_INTERNAL_KEYS * Long.BYTES;
        private static final int NEXT_LEAF_ADDRESS_OFFSET = PARTITION_ADDRESSES_OFFSET * 2;
        private static final int TOTAL_SIZE = NEXT_LEAF_ADDRESS_OFFSET + Long.BYTES;

        // there might be more compact representations than a full 8-byte long for the DK tokens, but this is already better
        // than needing to the DK compares for the whole tree.
        //
        // also, i don't think i'll need to store the token value ... except it will speed up the tree rebuild at startup
        // if i serialize it ...

        private final MemoryRegion region;

        private final long[] tokens;

        /**
         * This represents the actual address of the {@link PMemPartition}
         */
        private final long[] partitionAddresses;

        private int size;

        // primarily needed for rebuilding the tree at startup
        private long nextAddress;

        private PersistentLeaf(MemoryRegion region, long[] tokens, long[] partitionAddresses, long nextAddress, int size)
        {
            this.region = region;
            this.tokens = tokens;
            this.partitionAddresses = partitionAddresses;
            this.nextAddress = nextAddress;
            this.size = size;
        }

        static PersistentLeaf create(Heap heap, PmemTransaction tx)
        {
            PersistentLeaf[] leaf = new PersistentLeaf[1];
            tx.execute(() -> {
                MemoryRegion region = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, TOTAL_SIZE);
                byte[] bytes = new byte[MAX_LEAF_KEYS * Long.BYTES];
                ByteBuffer buf = ByteBuffer.wrap(bytes);

                // reusing readLongArray() as a naive way to get a reference to the bytes in the region
                long[] tokens = readLongArray(region, TOKENS_OFFSET, buf);
                long[] partitionAddresses = readLongArray(region, PARTITION_ADDRESSES_OFFSET, buf);
                leaf[0] = new PersistentLeaf(region, tokens, partitionAddresses, 0, 0);
            });
            return leaf[0];
        }

        /**
         * (re-)load a persistent leaf node from persistent memory, at the address provided.
         */
        static PersistentLeaf load(Heap heap, long address)
        {
            MemoryRegion region = heap.memoryRegionFromAddress(MemoryRegion.Kind.TRANSACTIONAL, address);
            byte[] bytes = new byte[MAX_LEAF_KEYS * Long.BYTES];
            ByteBuffer buf = ByteBuffer.wrap(bytes);

            long[] tokens = readLongArray(region, TOKENS_OFFSET, buf);
            long[] partitionAddresses = readLongArray(region, PARTITION_ADDRESSES_OFFSET, buf);
            long nextAddress = region.getLong(NEXT_LEAF_ADDRESS_OFFSET);

            // calculate the number of live tokens, by looking at the partition addrs.
            // no addr will be 0, but a token could legitimately be 0.
            int size = 0;
            for (int i = 0; i < MAX_INTERNAL_KEYS; i++)
            {
                if (partitionAddresses[i] == 0)
                    break;
                size++;
            }
            return new PersistentLeaf(region, tokens, partitionAddresses, nextAddress, size);
        }

        private static long[] readLongArray(MemoryRegion region, long offset, ByteBuffer buf)
        {
            // there's a less terrible way to do this ...
            buf.clear();
            region.getBuffer(offset, buf);

            buf.position(0);
            long[] longs = new long[MAX_LEAF_KEYS];
            for (int i = 0; i < MAX_LEAF_KEYS; i++)
                longs[i] = buf.getLong();
            return longs;
        }

        // assumes we're just insert new row(s) ... merge logic to be implemented ...
        void add(Heap heap, PartitionUpdate update, PmemTransaction tx)
        {
            int index = findInsertionIndex(update, tx);
            long partitionAddr = partitionAddresses[index];

            if (partitionAddr == 0)
            {
                // create new PmemAddr
                PMemPartition partition = PMemPartition.create(update, heap, tx);
                tx.execute(() -> {
                    tokens[index] = ((LongToken) update.partitionKey().getToken()).getLongValue();
                    partitionAddresses[index] = partition.getAddress();
                });
            }
            else
            {
                // handle the token collision case. Partition.next acts like a sorted linked list.
                PMemPartition partition = new PMemPartition(heap, partitionAddr, update.partitionKey().getToken());
                PMemPartition prevPartition = null;
                while (true)
                {
                    int cmp = partition.getDecoratedKey().compareTo(update.partitionKey());
                    if (cmp == 0)
                    {
                        break;
                    }
                    else if (cmp < 0)
                    {
                        PMemPartition next = partition;
                        partition = PMemPartition.create(update, heap, tx);
                        partition.setNext(next, tx);
                        if (prevPartition != null)
                        {
                            prevPartition.setNext(partition, tx);
                            // update the first address offset in the owning p-leaf
                            long paritionAddr = partition.getAddress();
                            tx.execute(() -> {
                                tokens[index] = ((LongToken) update.partitionKey().getToken()).getLongValue();
                                partitionAddresses[index] = paritionAddr;
                            });

                        }
                        break;
                    }

                    prevPartition = partition;
                    if (!partition.hasNext())
                    {
                        partition = PMemPartition.create(update, heap, tx);
                        prevPartition.setNext(partition, tx);
                        break;
                    }

                    partition = partition.next();
                }
            }
        }

        private int findInsertionIndex(PartitionUpdate update, PmemTransaction tx)
        {
            // find index to insert at ... just using 0 for now
            // this method will need to move entries if we need to insert between two values,
            // hence the need for th transacion

            // TODO; figure out how to handle when this leaf node if full ...
            // maybe return -1 as an inidcator to split me ??

            return 0;
        }
    }
}
