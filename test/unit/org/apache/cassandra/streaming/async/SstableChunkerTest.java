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

package org.apache.cassandra.streaming.async;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.DefaultFileRegion;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class SstableChunkerTest extends CQLTester
{
    private static final int KEY_COUNT = 100;
    private static final ByteBufAllocator ALLOCATOR = UnpooledByteBufAllocator.DEFAULT;

    /*
        functions to test:
        - hasNext/next
        - close ??
        - calculateNextPositions?
     */

    private ColumnFamilyStore insertData(String table) throws Throwable
    {
        for (int i = 0; i < KEY_COUNT; i++)
            execute("INSERT INTO %s (id, c1) VALUES (?, ?)", i, 42 * i);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
        cfs.forceBlockingFlush();
        return cfs;
    }

    private OutgoingFileMessage createOFM(SSTableReader sstable, String table)
    {
        List<Pair<Long, Long>> sections = new ArrayList<>(1);
        sections.add(Pair.create(0L, sstable.getDataChannel().size()));
        TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, table);
        SerializationHeader serializationHeader = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);
        FileMessageHeader header = new FileMessageHeader(metadata.id, UUID.randomUUID(), 1, 1, sstable.descriptor.version,
                                                         sstable.descriptor.formatType, KEY_COUNT, sections,
                                                         sstable.compression ? sstable.getCompressionMetadata() : null, 0, 0, serializationHeader.toComponent());
        OutgoingFileMessage ofm = new OutgoingFileMessage(header, sstable.ref(), sstable.getFilename());
        ofm.startTransfer();
        return ofm;
    }

    @Test
    public void uncompressed_NoValidation() throws Throwable
    {
        String table = createTable("create table %s (id int primary key, c1 int) with compression = {}");
        ColumnFamilyStore cfs = insertData(table);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        OutgoingFileMessage ofm = createOFM(sstable, table);

        SstableChunker chunker = new UncompressedSstableChunker(ALLOCATOR, ofm, null, SstableChunker.NON_ZERO_COPY_CHUNK_SIZE);
        Assert.assertTrue(chunker.hasNext());
        while (chunker.hasNext())
        {
            Object o = chunker.next();
            Assert.assertNotNull(o);
            ByteBuf buf = (ByteBuf)o;
            Assert.assertEquals(sstable.getDataChannel().size(), buf.readableBytes());
        }

        ofm.complete();
        ofm.finishTransfer();
    }

    @Test
    public void uncompressed_WithValidation() throws Throwable
    {
        String table = createTable("create table %s (id int primary key, c1 int) with compression = {}");
        ColumnFamilyStore cfs = insertData(table);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        OutgoingFileMessage ofm = createOFM(sstable, table);

        SstableChunker chunker = UncompressedSstableChunker.create(ALLOCATOR, ofm);
        Assert.assertTrue(chunker.hasNext());
        while (chunker.hasNext())
        {
            Object o = chunker.next();
            Assert.assertNotNull(o);
            ByteBuf buf = (ByteBuf)o;
            Assert.assertEquals(sstable.getDataChannel().size(), buf.readableBytes());
        }

        ofm.complete();
        ofm.finishTransfer();
    }

    @Test
    public void compressed_NoTLS() throws Throwable
    {
        String table = createTable("create table %s (id int primary key, c1 int) with compression = {'class':'LZ4Compressor'}");
        ColumnFamilyStore cfs = insertData(table);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        OutgoingFileMessage ofm = createOFM(sstable, table);

        SstableChunker chunker = new CompressedSstableChunker(ofm, ofm.header.getCompressionInfo());
        Assert.assertTrue(chunker.hasNext());
        while (chunker.hasNext())
        {
            Object o = chunker.next();
            Assert.assertNotNull(o);
            DefaultFileRegion buf = (DefaultFileRegion) o;
            Assert.assertEquals(sstable.getDataChannel().size(), buf.count());
        }

        ofm.complete();
        ofm.finishTransfer();
    }

    @Test
    public void compressed_WithTLS() throws Throwable
    {
        String table = createTable("create table %s (id int primary key, c1 int) with compression = {'class':'LZ4Compressor'}");
        ColumnFamilyStore cfs = insertData(table);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        OutgoingFileMessage ofm = createOFM(sstable, table);

        SstableChunker chunker = new SecureCompressedSstableChunker(ofm, ofm.header.getCompressionInfo(), UnpooledByteBufAllocator.DEFAULT);
        Assert.assertTrue(chunker.hasNext());
        while (chunker.hasNext())
        {
            Object o = chunker.next();
            Assert.assertNotNull(o);
            ByteBuf buf = (ByteBuf) o;
            Assert.assertEquals(sstable.getDataChannel().size(), buf.readableBytes());
        }

        ofm.complete();
        ofm.finishTransfer();
    }
}