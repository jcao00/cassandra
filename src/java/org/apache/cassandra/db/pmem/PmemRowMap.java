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

import lib.llpl.Heap;
import lib.llpl.MemoryRegion;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;

// keys are Clusterings, and are stored in a MemoryRegion seperate from values
// values are Rows, with the columns serialized (with assocaited header and column information) stored contiguously
// in one MemoryRegion
// -- EVENTUALLY, that's how it wil be stored. I've naively just invoked Cell.serializer.serialize for the cells ...


// basically, this is a sorted hash map, persistent and unsafe, specific to a given partition
public class PmemRowMap
{
    private final MemoryRegion region;
    private final Heap heap;

    private final PersistentEntry sentinel;

    public PmemRowMap(MemoryRegion region, Heap heap, PersistentEntry sentinel)
    {
        this.region = region;
        this.heap = heap;
        this.sentinel = sentinel;
    }

    public static PmemRowMap create(Heap heap, PmemTransaction tx)
    {
        PmemRowMap[] map = new PmemRowMap[1];
        tx.execute(() -> {
            int headerSize = 8;
            MemoryRegion region = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, headerSize);
            PersistentEntry sentinel = PersistentEntry.create(heap, tx);
            map[0] = new PmemRowMap(region, heap, sentinel);
        });
        return map[0];
    }

    private static class PersistentEntry
    {
        private static final int NEXT_ENTRY_ADDRESS_OFFSET = 0;
        private static final int PREVIOUS_ENTRY_ADDRESS_OFFSET = 8;
        private static final int KEY_ADDRESS_OFFSET = 16;
        private static final int ROW_ADDRESS_OFFSET = 24;
        private static final int HEADER_SIZE = 32;

        // doubly-linked list
        private final MemoryRegion region;
        private final Heap heap;

        private PersistentEntry(Heap heap, MemoryRegion region)
        {
            this.heap = heap;
            this.region = region;
        }

        private PersistentEntry(Heap heap, long address)
        {
            this.heap = heap;
            region = heap.memoryRegionFromAddress(MemoryRegion.Kind.TRANSACTIONAL, address);
        }

        private static PersistentEntry create(Heap heap, PmemTransaction tx)
        {
            return create(heap, tx, 0, 0);
        }

        private static PersistentEntry create(Heap heap, PmemTransaction tx, long keyAddress, long rowAddress)
        {
            PersistentEntry[] entry = new PersistentEntry[1];
            tx.execute(() -> {
                MemoryRegion region = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, HEADER_SIZE);
                region.putLong(KEY_ADDRESS_OFFSET, keyAddress);
                region.putLong(ROW_ADDRESS_OFFSET, rowAddress);
                entry[0] = new PersistentEntry(heap, region);
           });
            return entry[0];
        }

        private long getAddress()
        {
            return region.addr();
        }

        private Clustering getKey()
        {
            long address = region.getLong(KEY_ADDRESS_OFFSET);
            MemoryRegion keyRegion = heap.memoryRegionFromAddress(MemoryRegion.Kind.TRANSACTIONAL, address);
            MemoryRegionDataInputPlus inputPlus = new MemoryRegionDataInputPlus(keyRegion);
            inputPlus.position();
            return Clustering.serializer.deserialize(inputPlus, version, types);
        }

        private PersistentEntry next()
        {
            long address = region.getLong(NEXT_ENTRY_ADDRESS_OFFSET);
            return new PersistentEntry(heap, address);
        }

        // assumes a transaction is executing
        private void next(PersistentEntry entry)
        {
            region.putLong(NEXT_ENTRY_ADDRESS_OFFSET, entry.getAddress());
        }

        private PersistentEntry previous()
        {
            long address = region.getLong(PREVIOUS_ENTRY_ADDRESS_OFFSET);
            return new PersistentEntry(heap, address);
        }

        // assumes a transaction is executing
        private void previous(PersistentEntry entry)
        {
            region.putLong(PREVIOUS_ENTRY_ADDRESS_OFFSET, entry.getAddress());
        }

        private long nextAddress()
        {
            return region.getLong(NEXT_ENTRY_ADDRESS_OFFSET);
        }
    }

    public long getAddress()
    {
        region.addr();
    }

    // NOTE: just showing tyhe serialization path of the row, not anything map-related
    public void put(Row row, PmemTransaction tx)
    {
        /// KEY
        Clustering clustering = row.clustering();

        // need to get version number and list of absolute types here ...
        MemoryRegion keyRegion = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, Clustering.serializer.serializedSize());
        // wrap keyRegion in DOS
        MemoryRegionDataOutputPlus outputPlus = new MemoryRegionDataOutputPlus(keyRegion, 0);

        // need to get version number and list of absolute types here ...
        Clustering.serializer.serialize(clustering, outputPlus, -1, absTypes);

        /// VALUE
        // calcualte required size for memRegion
        int size = -1;
        MemoryRegion cellMemoryRegion = heap.allocateMemoryRegion(MemoryRegion.Kind.TRANSACTIONAL, size);
        MemoryRegionDataOutputPlus cellsOutputPlus = new MemoryRegionDataOutputPlus(cellMemoryRegion, 0);
        Iterable<Cell> cells = row.cells();
        for (Cell c : cells)
            Cell.serializer.serialize(c, , cellsOutputPlus, , , );

        long keyAddress = keyRegion.addr();
        long rowAddress = cellMemoryRegion.addr();

        PersistentEntry entry = PersistentEntry.create(heap, tx, keyAddress, rowAddress);
        insert(entry, tx , clustering);
    }

    // NOTE this is a totally ineffecient way of adding to the map (lineat scan), but you get the idea ....
    private void insert(PersistentEntry entry, PmemTransaction tx, Clustering clustering)
    {
        PersistentEntry cur = sentinel;
        while (true)
        {
            if (cur.nextAddress() == 0)
            {
                cur.next(entry);
                entry.previous(cur);
            }
            else
            {
                Clustering curKey = cur.getKey();
                int cmp = compare(curKey, clustering);
                if (cmp < 0)
                {
                    cur = cur.next();
                }
                else if (cmp > 0)
                {
                    PersistentEntry current = cur;
                    tx.execute(() -> {
                        PersistentEntry previous = current.previous();
                        previous.next(entry);
                        entry.previous(previous);

                        current.previous(entry);
                        entry.next(current);
                    });
                    break;
                }
                else
                {
                    throw new IllegalArgumentException();
                }
            }
        }
    }
}
