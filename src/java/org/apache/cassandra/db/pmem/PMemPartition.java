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
import java.util.Iterator;

import lib.llpl.Heap;
import lib.llpl.MemoryRegion;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;

/**
 * This struct is pointed to by the leaf nodes of the RBTree.
 *
 * The {@link #next} field deals with partitions whose keys happen to collide on the token. As that is a rarity,
 * a simple solution to handle the problem (essentially a linked list) should suffice as even if there are collisions,
 * there should be a tiny number of them ... else i guess you're screwed on that token (shrug)
 */
// TODO:JEB prolly rename me
public class PMemPartition
{
    /*
        binary format:
        - offset of partition key - 4 bytes
        // in the overwhelming majority of cases, this will be -1 (as it's rare to have a collision on token)
        - offset of partition delete info key - 4 bytes
        - address of maps of rows - 8 bytes
        - address of static row
        - address of 'next' partition - 8 bytes

        - decorated key
        -- length - 2 bytes
        -- data - length bytes
        - partition deletion data (DeletionTime.Serialier)
     */

    private static final int DECORATED_KEY_OFFSET = 0;  // int, relative to base address of this region
    private static final int DELETION_INFO_OFFSET = 4;  // int, relative to base address of this region
    private static final int ROW_MAP_ADDRESS = 8; // long, address
    private static final int STATIC_ROW_ADDRESS = 16;  // long, address
    private static final int NEXT_PARTITION_ADDRESS = 24;  // long, address
    private static final int HEADER_SIZE = 32;

    /**
     * The block in memory where the high-level data for this partition is stored.
     */
    private final MemoryRegion region;

    private final Token token;

    // memoized DK
    private DecoratedKey key;

    // memoized map  Clustering -> Row, where Row is serialized in to one blob
    // - allows compression (debatable if needed)
    // - requires a read, deserialize, append, write semantic for updating an existing row
//    private PersistentUnsafeHashMap<ByteBuffer, ByteBuffer> rows;

    private final Heap heap;

    PMemPartition(Heap heap, MemoryRegion region, Token token)
    {
        this.heap = heap;
        this.region = region;
        this.token= token;
    }

    public static PMemPartition load(Heap heap, Token token, long address)
    {
        MemoryRegion region = heap.memoryRegionFromAddress(MemoryRegion.Kind.TRANSACTIONAL, address);
        return new PMemPartition(heap, region, token);
    }

    public DecoratedKey getDecoratedKey()
    {
        if (key == null)
            key = new PmemDecoratedKey(token, region);
        return key;
    }

    public DeletionTime getPartitionDelete()
    {
        int offset = region.getInt(DELETION_INFO_OFFSET);
        if (offset <= 0)
            return DeletionTime.LIVE;

        MemoryRegionDataInputPlus inputPlus = new MemoryRegionDataInputPlus(region);
        inputPlus.position(offset);
        return DeletionTime.serializer.deserialize(inputPlus);
    }

    public PMemPartition next()
    {
        long nextAddress = region.getLong(NEXT_PARTITION_ADDRESS);
        if (nextAddress == 0)
            return null;
        return load(heap, token, nextAddress);
    }

    public boolean hasNext()
    {
        return region.getLong(NEXT_PARTITION_ADDRESS) != 0;
    }

    public void setNext(PMemPartition partition, PmemTransaction tx)
    {
        tx.execute(() -> region.putLong(NEXT_PARTITION_ADDRESS, partition.getAddress()));
    }

    public static PMemPartition create(PartitionUpdate update, Heap heap, PmemTransaction tx)
    {
        ByteBuffer key = update.partitionKey().getKey();
        RegularAndStaticColumns columns = update.columns();

        // handle static row seperate from regular row(s)
        final long staticRowAddress;
        if (!update.staticRow().isEmpty())
        {
            long[] staticRowAddressArray = new long[1];
            tx.execute(() -> {

                MemoryRegion staticRowRegion = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, );


                staticRowAddressArray[0] = staticRowRegion.addr();
            });
            staticRowAddress = staticRowAddressArray[0];
        }
        else
        {
            staticRowAddress = 0;
        }

        /// ROWS!!!
        final long rowMapAddress;
        if (update.hasRows())
        {
            PmemRowMap rm = PmemRowMap.create(heap, tx);
            rowMapAddress = rm.getAddress();

            // build the headers data from this

            for (Row r : update)
                rm.put(r, tx);
        }
        else
        {
            rowMapAddress = 0;
        }

        PMemPartition[] partition = new PMemPartition[1];
        tx.execute(() -> {
            int keySize = Short.BYTES + key.limit();

            DeletionTime partitionDelete = update.deletionInfo().getPartitionDeletion();
            int partitionDeleteSize = partitionDelete.isLive() ? 0 : (int) DeletionTime.serializer.serializedSize(partitionDelete);
            int size = HEADER_SIZE + keySize + partitionDeleteSize;


            // TODO there's some clean up/correctness work to be done here, but the basic idea is right
            MemoryRegion region = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, size);
            MemoryRegionDataOutputPlus outputPlus = new MemoryRegionDataOutputPlus(region, 0);

            /// partition-level delete
            if (partitionDeleteSize == 0)
            {
                region.putInt(DELETION_INFO_OFFSET, 0);
            }
            else
            {
                int offset = (int) region.addr();
                DeletionTime.serializer.serialize(deletionInfo, outputPlus);
                region.putInt(DELETION_INFO_OFFSET, offset);
            }

            region.putLong(ROW_MAP_ADDRESS, rowMapAddress);
            region.putLong(NEXT_PARTITION_ADDRESS, 0);
            region.putLong(STATIC_ROW_ADDRESS, staticRowAddress);

            region.putShort(DECORATED_KEY_OFFSET, (short) key.remaining());
            region.putBuffer(DECORATED_KEY_OFFSET + Short.BYTES, key);

            partition[0] = new PMemPartition(heap,region, update.partitionKey().getToken());
        });
        return partition[0];
    }


    // AtomicBTreePartition.addAllWithSizeDelta & RowUpdater are the places to look to see how classic storage engine stashes things

    public long getAddress()
    {
        return region.addr();
    }

    public static class PmemDecoratedKey extends DecoratedKey
    {
        private final MemoryRegion region;

        PmemDecoratedKey(Token token, MemoryRegion memoryRegion)
        {
            super(token);
            this.region = memoryRegion;
        }

        @Override
        public ByteBuffer getKey()
        {
            int position = region.getInt(DECORATED_KEY_OFFSET);
            int size = region.getShort(position);
            ByteBuffer buf = ByteBuffer.allocate(size);
            buf.limit(size);
            region.getBuffer(DECORATED_KEY_OFFSET + Short.BYTES, buf);
            return buf;
        }
    }
}
