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

package org.apache.cassandra.io.sstable.format;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableWriterTestBase;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

public class EncryptedSSTableTest extends SSTableWriterTestBase
{
    private static final int maxPartitons = 1024;
    private static final int maxCqlRows = 32;
    private static final String colName = "val";

    private static boolean hasSetEncryption;

    private ColumnFamilyStore cfs;

    @Before
    public void setUp()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        cfs = keyspace.getColumnFamilyStore(CF);

        if (!hasSetEncryption)
        {
            hasSetEncryption = true;
            StorageService.instance.initServer();
            DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createContext(true));

            TableParams tableParams = TableParams.builder().encryption(true).build();
            cfs.metadata.params(tableParams);
        }

        truncate(cfs);
    }

    @Test
    public void insertFlushAndRead() throws CharacterCodingException
    {
        insertAndFlush();
        checkSstable(checkCfs());
    }

    private void insertAndFlush()
    {
        for (int i = 0; i < maxPartitons; i++)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata, ByteBufferUtil.bytes(i));
            for (int j = 0; j < maxCqlRows; j++)
                builder.newRow(ByteBufferUtil.bytes(Integer.toString(j))).add(colName, ByteBufferUtil.bytes(j));
            builder.apply();
        }

        cfs.forceBlockingFlush();
    }

    private SSTableReader checkCfs()
    {
        Assert.assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        Assert.assertTrue(sstable.compression);
        Assert.assertTrue(sstable.indexCompression);
        return sstable;
    }

    private void checkSstable(SSTableReader sstable)
    {
        for (int i = 0; i < maxPartitons; i++)
        {
            DecoratedKey key = cfs.getPartitioner().decorateKey(ByteBufferUtil.bytes(i));
            int cqlRowCount;
            try (UnfilteredRowIterator iterator = sstable.iterator(key, Slices.ALL, ColumnFilter.all(cfs.metadata), false, false))
            {
                Assert.assertEquals(key, iterator.partitionKey());

                cqlRowCount = 0;
                while (iterator.hasNext())
                {
                    Unfiltered unfiltered = iterator.next();
                    Assert.assertTrue(unfiltered.isRow());
                    Row row = (Row) unfiltered;

                    Collection<ColumnDefinition> columns = row.columns();
                    Assert.assertEquals(1, columns.size());
                    ColumnDefinition def = columns.iterator().next();
                    Assert.assertEquals(colName, def.name.toCQLString());


                    Cell cell = row.getCell(def);
                    ByteBufferUtil.toInt(cell.value());

                    cqlRowCount++;
                }
            }
            Assert.assertEquals(maxCqlRows, cqlRowCount);
        }
    }

    // basic structure of this test is borrowed from StreamingTransferTest
    @Test
    public void stream() throws ExecutionException, InterruptedException
    {
        insertAndFlush();
        SSTableReader sstable = checkCfs();
        cfs.clearUnsafe();
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());

        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes(Integer.toString(maxCqlRows)))));

        Refs<SSTableReader> ssTableReaders = Refs.tryRef(Collections.singletonList(sstable));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(ssTableReaders.get(sstable),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("EncryptedStreamingTransferTest").transferFiles(FBUtilities.getBroadcastAddress(), details).execute().get();

        checkSstable(checkCfs());
    }
}