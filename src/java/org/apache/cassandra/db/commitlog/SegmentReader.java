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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tjake.ICRC32;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ByteBufferDataInput;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CRC32Factory;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.SYNC_MARKER_SIZE;

/**
 * Read each sync section of a commit log, iteratively.
 */
public class SegmentReader implements Iterable<SegmentReader.SyncSegment>
{
    private static final Logger logger = LoggerFactory.getLogger(SegmentReader.class);

    private final CommitLogDescriptor descriptor;
    private final RandomAccessReader reader;
    private final Segmenter segmenter;

    /**
     * ending position of the current sync section.
     */
    protected int end;

    protected SegmentReader(CommitLogDescriptor descriptor, RandomAccessReader reader)
    {
        this.descriptor = descriptor;
        this.reader = reader;

        end = (int) reader.getFilePointer();
        if (descriptor.getEncryptionContext().isEnabled())
            segmenter = new EncryptedSegmenter(reader, descriptor);
        else if (descriptor.compression != null)
            segmenter = new CompressedSegmenter(descriptor, reader);
        else
            segmenter = new NoOpSegmenter(reader);
    }

    public Iterator<SyncSegment> iterator()
    {
        return new SegmentIterator();
    }

    protected class SegmentIterator extends AbstractIterator<SegmentReader.SyncSegment>
    {
        protected SyncSegment computeNext()
        {
            try
            {
                final int currentStart = end;
                end = readSyncMarker(descriptor, currentStart, reader);
                if (end == -1)
                {
                    return endOfData();
                }
                if (end > reader.length())
                {
                    // the CRC was good (meaning it was good when it was written and still looks legit), but the file is truncated now.
                    // try to grab and use as much of the file as possible, which might be nothing if the end of the file truly is corrupt
                    end = (int) reader.length();
                }

                return segmenter.nextSegment(currentStart + SYNC_MARKER_SIZE, end);
            }
            catch (IOException e)
            {
                logger.warn("failed to parse commit log file", e);
                return endOfData();
            }
        }
    }

    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - SYNC_MARKER_SIZE)
        {
            // There was no room in the segment to write a final header. No data could be present here.
            return -1;
        }
        reader.seek(offset);
        ICRC32 crc = CRC32Factory.instance.create();
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        crc.updateInt((int) reader.getPosition());
        final int end = reader.readInt();
        long filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                logger.warn("Encountered bad header at position {} of commit log {}, with invalid CRC. The end of segment marker should be zero.\n", offset, reader.getPath());
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            logger.warn("Encountered bad header at position {} of commit log {}, with bad position but valid CRC", offset, reader.getPath());
            return -1;
        }
        return end;
    }

    public static class SyncSegment
    {
        /** the 'buffer' to replay commit log data from */
        public final FileDataInput input;

        /** the logical ending position of the buffer */
        public final int endPosition;

        public SyncSegment(FileDataInput input, int endPosition)
        {
            this.input = input;
            this.endPosition = endPosition;
        }
    }

    /**
     * Derives the next section of the commit log to be replayed. Section boundaries are derived from the commit log sync markers.
     */
    interface Segmenter
    {
        /**
         * Get the next section of the commit log to replay.
         *
         * @param startPosition the position in the file to begin reading at
         * @param nextSectionStartPosition the file position of the beginning of the next section
         * @return the buffer and it's logical end position
         * @throws IOException
         */
        SyncSegment nextSegment(int startPosition, int nextSectionStartPosition) throws IOException;
    }

    static class NoOpSegmenter implements Segmenter
    {
        private final RandomAccessReader reader;

        public NoOpSegmenter(RandomAccessReader reader)
        {
            this.reader = reader;
        }

        public SyncSegment nextSegment(int startPosition, int nextSectionStartPosition)
        {
            reader.seek(startPosition);
            return new SyncSegment(reader, nextSectionStartPosition);
        }
    }

    static class CompressedSegmenter implements Segmenter
    {
        private final ICompressor compressor;
        private final RandomAccessReader reader;
        private byte[] compressedBuffer;
        private byte[] uncompressedBuffer;
        private long nextLogicalStart;

        public CompressedSegmenter(CommitLogDescriptor desc, RandomAccessReader reader)
        {
            this(CompressionParameters.createCompressor(desc.compression), reader);
        }

        public CompressedSegmenter(ICompressor compressor, RandomAccessReader reader)
        {
            this.compressor = compressor;
            this.reader = reader;
            compressedBuffer = new byte[0];
            uncompressedBuffer = new byte[0];
            nextLogicalStart = reader.getFilePointer();
        }

        public SyncSegment nextSegment(final int startPosition, final int nextSectionStartPosition) throws IOException
        {
            reader.seek(startPosition);
            int uncompressedLength = reader.readInt();

            int compressedLength = nextSectionStartPosition - (int)reader.getPosition();
            if (compressedLength > compressedBuffer.length)
                compressedBuffer = new byte[(int) (1.2 * compressedLength)];
            reader.readFully(compressedBuffer, 0, compressedLength);

            if (uncompressedLength > uncompressedBuffer.length)
               uncompressedBuffer = new byte[(int) (1.2 * uncompressedLength)];
            int count = compressor.uncompress(compressedBuffer, 0, compressedLength, uncompressedBuffer, 0);
            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new ByteBufferDataInput(ByteBuffer.wrap(uncompressedBuffer, 0, count), reader.getPath(), nextLogicalStart, 0);
            nextLogicalStart += uncompressedLength;
            return new SyncSegment(input, (int)nextLogicalStart);
        }
    }

    static class EncryptedSegmenter implements Segmenter
    {
        private final RandomAccessReader reader;
        private final ICompressor compressor;
        private final Cipher cipher;

        /**
         * we read the raw encrypted bytes, from the file, into this buffer.
         */
        private ByteBuffer fileReadBuffer;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer decryptedBuffer;

        /**
         * the result of the decryption is written into this buffer.
         */
        private ByteBuffer uncompressedBuffer;

        private long nextLogicalStart;
        private final Function<ByteBuffer, ByteBuffer> decryptFunction;

        public EncryptedSegmenter(RandomAccessReader reader, CommitLogDescriptor descriptor)
        {
            this(reader, descriptor.getEncryptionContext(), (String)descriptor.headerParameters.get(EncryptionContext.ENCRYPTION_IV));
        }

        @VisibleForTesting
        EncryptedSegmenter(RandomAccessReader reader, EncryptionContext encryptionContext, String iv)
        {
            this.reader = reader;
            fileReadBuffer = ByteBuffer.allocate(0);
            decryptedBuffer = ByteBuffer.allocate(0);
            compressor = encryptionContext.getCompressor();
            nextLogicalStart = reader.getFilePointer();

            try
            {
                if (iv == null || iv.isEmpty())
                    throw new IllegalStateException("no initialization vector (IV) store with commit log, and cannot initialize decrypt operations");

                cipher = encryptionContext.getDecryptor(Hex.hexToBytes(iv));
            }
            catch (IOException ioe)
            {
                throw new FSReadError(ioe, reader.getPath());
            }

            decryptFunction = new Function<ByteBuffer, ByteBuffer>()
            {
                public ByteBuffer apply(ByteBuffer input)
                {
                    if (input.remaining() == 0)
                        return null;
                    try
                    {
                        decryptedBuffer = CommitLogEncryptionUtils.decrypt(input, decryptedBuffer, true, cipher);
                        uncompressedBuffer = CommitLogEncryptionUtils.uncompress(decryptedBuffer, uncompressedBuffer, true, compressor);
                        return uncompressedBuffer;
                    }
                    catch (IOException | ShortBufferException | BadPaddingException | IllegalBlockSizeException e)
                    {
                        throw new FSReadError(e, reader.getPath());
                    }
                }
            };
        }

        public SyncSegment nextSegment(final int startPosition, final int nextSectionStartPosition) throws IOException
        {
            // read the entire sync segment in one go; then, iterate for each encrypted block
            int length = nextSectionStartPosition - startPosition;
            fileReadBuffer = ByteBufferUtil.ensureCapacity(fileReadBuffer, length, true);
            reader.getChannel().readFully(fileReadBuffer, startPosition);
            fileReadBuffer.flip();
            int plainTextLength = fileReadBuffer.getInt();

            nextLogicalStart += SYNC_MARKER_SIZE;
            FileDataInput input = new RebufferingDataInput(fileReadBuffer, reader.getPath(), nextLogicalStart, 0, plainTextLength, decryptFunction);
            nextLogicalStart += plainTextLength;
            return new SyncSegment(input, (int)nextLogicalStart);
        }
    }
}