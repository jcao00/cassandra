/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.db.commitlog;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CommitLogTest
{
    public static final String KEYSPACE1 = "CommitLogTest";
    public static final String KEYSPACE2 = "CommitLogTestNonDurable";
    public static final String CF1 = "Standard1";
    public static final String CF2 = "Standard2";

    String logDirectory;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    false,
                                    true,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2));
        System.setProperty("cassandra.commitlog.stop_on_errors", "true");
        CompactionManager.instance.disableAutoCompaction();
    }

    @Before
    public void setup()
    {
        logDirectory = DatabaseDescriptor.getCommitLogLocation() + "/unit";
        new File(logDirectory).mkdirs();
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        CommitLog.instance.recover(tmpFile().left);
    }

    @Test
    public void testRecoveryWithShortLog() throws Exception
    {
        // force EOF while reading log
        testRecoveryWithBadSizeArgument(100, 10);
    }

    @Test
    public void testRecoveryWithShortSize() throws Exception
    {
        testRecovery(new byte[2]);
    }

    @Test
    public void testRecoveryWithShortCheckSum() throws Exception
    {
        testRecovery(new byte[6]);
    }

    @Test
    public void testRecoveryWithGarbageLog() throws Exception
    {
        byte[] garbage = new byte[100];
        (new java.util.Random()).nextBytes(garbage);
        testRecovery(garbage);
    }

    @Test
    public void testRecoveryWithBadSizeChecksum() throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(100);
        testRecoveryWithBadSizeArgument(100, 100, ~checksum.getValue());
    }

    @Test
    public void testRecoveryWithZeroSegmentSizeArgument() throws Exception
    {
        // many different combinations of 4 bytes (garbage) will be read as zero by readInt()
        testRecoveryWithBadSizeArgument(0, 10); // zero size, but no EOF
    }

    @Test
    public void testRecoveryWithNegativeSizeArgument() throws Exception
    {
        // garbage from a partial/bad flush could be read as a negative size even if there is no EOF
        testRecoveryWithBadSizeArgument(-10, 10); // negative size, but no EOF
    }

    @Test
    public void testDontDeleteIfDirty() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        // Roughly 32 MB mutation
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 4), 0);

        // Adding it 5 times
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        // Adding new mutation on another CF
        Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add(CF2, Util.cellname("c1"), ByteBuffer.allocate(4), 0);
        CommitLog.instance.add(rm2);

        Assert.assertEquals(2, CommitLog.instance.activeSegments());

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segments
        Assert.assertEquals(2, CommitLog.instance.activeSegments());
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        DatabaseDescriptor.getCommitLogSegmentSize();
        CommitLog.instance.resetUnsafe(true);
        // Roughly 32 MB mutation
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize() / 4) - 1), 0);

        // Adding it twice (won't change segment)
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        Assert.assertEquals(1, CommitLog.instance.activeSegments());

        // "Flush": this won't delete anything
        UUID cfid1 = rm.getColumnFamilyIds().iterator().next();
        CommitLog.instance.sync(true);
        CommitLog.instance.discardCompletedSegments(cfid1, CommitLog.instance.getContext());

        Assert.assertEquals(1, CommitLog.instance.activeSegments());

        // Adding new mutation on another CF, large enough (including CL entry overhead) that a new segment is created
        Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add(CF2, Util.cellname("c1"), ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize() / 2) - 200), 0);
        CommitLog.instance.add(rm2);
        // also forces a new segment, since each entry-with-overhead is just under half the CL size
        CommitLog.instance.add(rm2);
        CommitLog.instance.add(rm2);

        Assert.assertEquals(3, CommitLog.instance.activeSegments());

        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        Assert.assertEquals(1, CommitLog.instance.activeSegments());
    }

    private static int getMaxRecordDataSize(String keyspace, ByteBuffer key, String table, CellName column)
    {
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add("Standard1", Util.cellname("c1"), ByteBuffer.allocate(0), 0);

        int max = (DatabaseDescriptor.getCommitLogSegmentSize() / 2);
        max -= CommitLogSegment.ENTRY_OVERHEAD_SIZE; // log entry overhead
        return max - (int) Mutation.serializer.serializedSize(rm, MessagingService.current_version);
    }

    private static int getMaxRecordDataSize()
    {
        return getMaxRecordDataSize(KEYSPACE1, bytes("k"), CF1, Util.cellname("c1"));
    }

    // CASSANDRA-3615
    @Test
    public void testEqualRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);

        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(getMaxRecordDataSize()), 0);
        CommitLog.instance.add(rm);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceedRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(1 + getMaxRecordDataSize()), 0);
        CommitLog.instance.add(rm);
    }

    protected void testRecoveryWithBadSizeArgument(int size, int dataSize) throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(size);
        testRecoveryWithBadSizeArgument(size, dataSize, checksum.getValue());
    }

    protected void testRecoveryWithBadSizeArgument(int size, int dataSize, long checksum) throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(size);
        dout.writeLong(checksum);
        dout.write(new byte[dataSize]);
        dout.close();
        testRecovery(out.toByteArray());
    }

    /**
     * Create a temporary commit log file with an appropriate descriptor at the head.
     *
     * @return the commit log file reference and the first position after the descriptor in the file
     * (so that subsequent writes happen at the correct file location).
     */
    protected Pair<File, Integer> tmpFile() throws IOException
    {
        EncryptionContext encryptionContext = DatabaseDescriptor.getEncryptionContext();
        CommitLogDescriptor desc = new CommitLogDescriptor(CommitLogDescriptor.current_version,
                                                           CommitLogSegment.getNextId(),
                                                           DatabaseDescriptor.getCommitLogCompression(),
                                                           encryptionContext);

        // if we're testing encryption, we need to write out a cipher IV to the descriptor headers
        Map<String, String> additionalHeaders = new HashMap<>();
        if (encryptionContext.isEnabled())
        {
            byte[] buf = new byte[16];
            new Random().nextBytes(buf);
            additionalHeaders.put(EncryptionContext.ENCRYPTION_IV, Hex.bytesToHex(buf));
        }

        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc, additionalHeaders);
        buf.flip();
        int positionAfterHeader = buf.limit() + 1;

        File logFile = new File(logDirectory, desc.fileName());
        logFile.deleteOnExit();

        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(buf.array(), 0, buf.limit());
        }

        return Pair.create(logFile, positionAfterHeader);
    }

    protected void testRecovery(byte[] logData) throws Exception
    {
        Pair<File, Integer> pair = tmpFile();
        try (RandomAccessFile raf = new RandomAccessFile(pair.left, "rw"))
        {
            raf.seek(pair.right);
            raf.write(logData);
            raf.close();

            CommitLog.instance.recover(new File[]{ pair.left }); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
        }
    }

    @Test
    public void testTruncateWithoutSnapshot() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        boolean prev = DatabaseDescriptor.isAutoSnapshot();
        DatabaseDescriptor.setAutoSnapshot(false);
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard2");

        final Mutation rm1 = new Mutation(KEYSPACE1, bytes("k"));
        rm1.add("Standard1", Util.cellname("c1"), ByteBuffer.allocate(100), 0);
        rm1.apply();
        cfs1.truncateBlocking();
        DatabaseDescriptor.setAutoSnapshot(prev);
        final Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add("Standard2", Util.cellname("c1"), ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 4), 0);

        for (int i = 0; i < 5; i++)
            CommitLog.instance.add(rm2);

        Assert.assertEquals(2, CommitLog.instance.activeSegments());
        ReplayPosition position = CommitLog.instance.getContext();
        for (Keyspace ks : Keyspace.system())
            for (ColumnFamilyStore syscfs : ks.getColumnFamilyStores())
                CommitLog.instance.discardCompletedSegments(syscfs.metadata.cfId, position);
        CommitLog.instance.discardCompletedSegments(cfs2.metadata.cfId, position);
        Assert.assertEquals(1, CommitLog.instance.activeSegments());
    }

    @Test
    public void testTruncateWithoutSnapshotNonDurable() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        boolean prevAutoSnapshot = DatabaseDescriptor.isAutoSnapshot();
        DatabaseDescriptor.setAutoSnapshot(false);
        Keyspace notDurableKs = Keyspace.open(KEYSPACE2);
        Assert.assertFalse(notDurableKs.metadata.durableWrites);
        ColumnFamilyStore cfs = notDurableKs.getColumnFamilyStore("Standard1");
        CellNameType type = notDurableKs.getColumnFamilyStore("Standard1").getComparator();
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation(KEYSPACE2, dk.getKey());
        rm.add("Standard1", Util.cellname("Column1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand(KEYSPACE2, dk.getKey(), "Standard1", System.currentTimeMillis(), new NamesQueryFilter(FBUtilities.singleton(Util.cellname("Column1"), type)));
        Row row = command.getRow(notDurableKs);
        Cell col = row.cf.getColumn(Util.cellname("Column1"));
        Assert.assertEquals(col.value(), ByteBuffer.wrap("abcd".getBytes()));
        cfs.truncateBlocking();
        DatabaseDescriptor.setAutoSnapshot(prevAutoSnapshot);
        row = command.getRow(notDurableKs);
        Assert.assertEquals(null, row.cf);
    }

    @Test
    public void replay_StandardMmapped() throws IOException
    {
        DatabaseDescriptor.setCommitLogCompression(null);
        DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createDisabledContext());
        CommitLog commitLog = new CommitLog(logDirectory, CommitLog.instance.archiver);
        replaySimple(commitLog);
        replayWithDiscard(commitLog);
    }

    @Test
    public void replay_Compressed_LZ4() throws IOException
    {
        replay_Compressed(new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()));
    }

    @Test
    public void replay_Compressed_Snappy() throws IOException
    {
        replay_Compressed(new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()));
    }

    @Test
    public void replay_Compressed_Deflate() throws IOException
    {
        replay_Compressed(new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()));
    }

    private void replay_Compressed(ParameterizedClass parameterizedClass) throws IOException
    {
        DatabaseDescriptor.setCommitLogCompression(parameterizedClass);
        DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createDisabledContext());
        CommitLog commitLog = new CommitLog(logDirectory, CommitLog.instance.archiver);
       // replaySimple(commitLog);
        replayWithDiscard(commitLog);
    }

    @Test
    public void replay_Encrypted() throws IOException
    {
        DatabaseDescriptor.setCommitLogCompression(null);
        DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createContext(true));
        CommitLog commitLog = new CommitLog(logDirectory, CommitLog.instance.archiver);

        try
        {
            replaySimple(commitLog);
            replayWithDiscard(commitLog);
        }
        finally
        {
            for (String file : commitLog.getActiveSegmentNames())
                FileUtils.delete(new File(commitLog.location, file));
        }
    }

    private void replaySimple(CommitLog commitLog) throws IOException
    {
        int cellCount = 0;
        final Mutation rm1 = new Mutation(KEYSPACE1, bytes("k1"));
        rm1.add(CF1, Util.cellname("c1"), ByteBufferUtil.bytes("this is a string"), 0);
        cellCount += 1;
        commitLog.add(rm1);

        final Mutation rm2 = new Mutation(KEYSPACE1, bytes("k2"));
        rm2.add(CF1, Util.cellname("c2"), ByteBufferUtil.bytes("this is a string"), 0);
        cellCount += 1;
        commitLog.add(rm2);

        commitLog.sync(true);

        Replayer replayer = new Replayer(ReplayPosition.NONE);
        List<String> activeSegments = commitLog.getActiveSegmentNames();
        Assert.assertFalse(activeSegments.isEmpty());

        for (String file : activeSegments)
            replayer.recover(new File(commitLog.location, file));

        Assert.assertEquals(cellCount, replayer.cells);
    }

    private void replayWithDiscard(CommitLog commitLog) throws IOException
    {
        int cellCount = 0;
        int max = 1024;
        int discardPosition = (int)(max * .8); // an arbitrary number of entries that we'll skip on the replay
        ReplayPosition replayPosition = null;

        for (int i = 0; i < max; i++)
        {
            final Mutation rm1 = new Mutation(KEYSPACE1, bytes("k" + i));
            rm1.add(CF1, Util.cellname("c1"), ByteBufferUtil.bytes("this is a string"), 0);
            ReplayPosition position = commitLog.add(rm1);
            System.out.format("***** on write, pos = %d\n", position.position);

            if (i == discardPosition)
                replayPosition = position;
            if (i > discardPosition)
            {
                cellCount += 1;
            }
        }

        commitLog.sync(true);
        System.out.format("***** discard position = %d\n", replayPosition.position);

        Replayer replayer = new Replayer(replayPosition);
        List<String> activeSegments = commitLog.getActiveSegmentNames();
        Assert.assertFalse(activeSegments.isEmpty());

        for (String file : activeSegments)
            replayer.recover(new File(commitLog.location, file));

        Assert.assertEquals(cellCount, replayer.cells);
    }

    class Replayer extends CommitLogReplayer
    {
        private final ReplayPosition filterPosition;
        int cells;
        int skipped;

        Replayer(ReplayPosition filterPosition)
        {
            super(filterPosition, null, ReplayFilter.create());
            this.filterPosition = filterPosition;
        }

        void replayMutation(byte[] inputBuffer, int size, final long entryLocation, final CommitLogDescriptor desc) throws IOException
        {
            System.out.format("**** entry loc = %d\n", entryLocation);
            if (entryLocation <= filterPosition.position)
            {
                // Skip over this mutation.
                skipped++;
                return;
            }
            else
            {
                int f = 4;
            }

            FastByteArrayInputStream bufIn = new FastByteArrayInputStream(inputBuffer, 0, size);
            Mutation mutation = Mutation.serializer.deserialize(new DataInputStream(bufIn), desc.getMessagingVersion(), ColumnSerializer.Flag.LOCAL);
            for (ColumnFamily cf : mutation.getColumnFamilies())
                cells += cf.getSortedColumns().size();
        }
    }
}
