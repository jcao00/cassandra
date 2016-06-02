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
package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Writes encrypted commit log segments to disk. Data is compressed before encrypting to (hopefully) reduce the size of the data into
 * the encryption algorithms.
 *
 * The format of the encrypted commit log is as follows:
 * - standard commit log header (as written by {@link CommitLogDescriptor#writeHeader(ByteBuffer, CommitLogDescriptor)})
 * - a series of 'sync segments' that are written every time the commit log is sync()'ed
 * -- a sync section header, see {@link CommitLogSegment#writeSyncMarker(ByteBuffer, int, int, int)}
 * -- total plain text length for this section
 * -- a series of encrypted data blocks, the format of each is described in {@link EncryptionContext}.
 */
public class EncryptedSegment extends FileDirectSegment
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSegment.class);

    private static final int ENCRYPTED_SECTION_HEADER_SIZE = SYNC_MARKER_SIZE + 4;

    private final EncryptionContext encryptionContext;

    public EncryptedSegment(CommitLog commitLog, EncryptionContext encryptionContext, Runnable onClose)
    {
        super(commitLog, onClose);
        this.encryptionContext = encryptionContext;
        logger.debug("created a new encrypted commit log segment: {}", logFile);
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        //Note: we want to keep the compression buffers on-heap as we need those bytes for encryption,
        // and we want to avoid copying from off-heap (compression buffer) to on-heap encryption APIs
        return createBuffer(BufferType.ON_HEAP);
    }

    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        final int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        final int blockSize = encryptionContext.getChunkLength();
        try
        {
            ByteBuffer inputBuffer = buffer.duplicate();
            inputBuffer.clear();
            inputBuffer.limit(contentStart + length).position(contentStart);

            // save space for the sync marker at the beginning of this section
            final long syncMarkerPosition = lastWrittenPos;
            channel.position(syncMarkerPosition + ENCRYPTED_SECTION_HEADER_SIZE);

            // loop over the segment data in encryption buffer sized chunks
            while (contentStart < nextMarker)
            {
                int nextBlockSize = nextMarker - blockSize > contentStart ? blockSize : nextMarker - contentStart;
                ByteBuffer slice = inputBuffer.duplicate();
                slice.limit(contentStart + nextBlockSize).position(contentStart);

                int writeCount = encryptionContext.encryptAndWrite(slice, channel);
                contentStart += nextBlockSize;
                commitLog.allocator.addSize(writeCount);
            }

            lastWrittenPos = channel.position();

            // rewind to the beginning of the section and write out the sync marker
            ByteBuffer writeBuffer = ByteBuffer.allocate(ENCRYPTED_SECTION_HEADER_SIZE);
            writeSyncMarker(writeBuffer, 0, (int) syncMarkerPosition, (int) lastWrittenPos);
            writeBuffer.putInt(SYNC_MARKER_SIZE, length);
            writeBuffer.position(0).limit(ENCRYPTED_SECTION_HEADER_SIZE);
            channel.position(syncMarkerPosition);
            channel.write(writeBuffer);

            commitLog.allocator.addSize(ENCRYPTED_SECTION_HEADER_SIZE);
            SyncUtil.force(channel, true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public long onDiskSize()
    {
        return lastWrittenPos;
    }
}
