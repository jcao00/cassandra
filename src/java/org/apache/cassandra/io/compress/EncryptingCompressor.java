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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.CompressedSegmentedFile;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionUtils;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_CIPHER;
import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_IV;
import static org.apache.cassandra.security.EncryptionContext.ENCRYPTION_KEY_ALIAS;

/**
 * Custom {@link ICompressor} implementation that performs encryption in addition to compression.
 * Unlike other {@link ICompressor} implementations, this class requires state to be maintained
 * (the current {@link EncryptionContext}), and thus needs a bit of special care.
 *
 * Each sstable will need a unique instance of this class, as the metadata about encryption will be different for each.
 * Further, as each sstable can be read by multiple threads, we need thread-local instances of the {@link Cipher},
 * as Cipher is not thread-safe (it's not documented that Cipher is not thread safe, but a simple test run wil bear that out).
 * As a last wrinkle, when an sstable is being written, and subsequently opened (either early, or otherwise),
 * the instance of this class will be reused from writing to reading stages (see {@link CompressedSegmentedFile.Builder#metadata(String, long)}.
 */
public class EncryptingCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(EncryptingCompressor.class);

    private static final Set<String> ENCRYPTION_OPTIONS = ImmutableSet.of(ENCRYPTION_CIPHER, ENCRYPTION_IV, ENCRYPTION_KEY_ALIAS);

    /**
     * Context specific to encrypting
     */
    private final EncryptionContext encryptionContext;

    /**
     * Context specific to decrypting
     */
    private final EncryptionContext decryptionContext;
    private final ICompressor compressor;

    // write (encrypting) state fields. Assumes writing is single-threaded
    private final Cipher encryptCipher;
    private ByteBuffer writeBuffer;

    // read (decrypting) state fields. allows for multi-threaded reading.
    private final ThreadLocal<ThreadReadState> readStates = new ThreadLocal<>();

    public EncryptingCompressor(Map<String, String> options, EncryptionContext baseEncryptionContext)
    {
        if (!options.containsKey(ENCRYPTION_CIPHER) || !options.containsKey(ENCRYPTION_KEY_ALIAS))
            throw new IllegalArgumentException("must set both the cipher algorithm and key alias");

        try
        {
            // ENCRYPTION_IV will only be null when creating an instance that is used to encrypt data,
            // (due to a memtable flush, compaction, streaming operation, etc). In that case, an IV will be automcatically
            // created in the {@code encryptCipher}, so we can immediately reuse it for the {@code decryptCipher}.
            // {@code decryptCipher} is eagerly created as immediately after encryption is complete (writing an sstable)
            // we will open immediately open it and can continue to use this instance.
            //
            // However, if ENCRYPTION_IV is present in the map, then we should be reading an existing sstable, and hence
            // do not need the {@code encryptCipher}.
            String ivString = options.get(ENCRYPTION_IV);

            // we are in a writing context (as IV was not provided)
            if (ivString == null)
            {
                encryptionContext = EncryptionContext.createFromMap(options, baseEncryptionContext);
                encryptCipher = encryptionContext.getEncryptor();
                decryptionContext = new EncryptionContext(baseEncryptionContext.getTransparentDataEncryptionOptions(), encryptCipher.getIV(), true);
                compressor = encryptionContext.getCompressor();
            }
            else
            {
                //we're in a read context if the IV is provided, so skip all the encryption components
                encryptionContext = null;
                encryptCipher = null;
                decryptionContext = EncryptionContext.createFromMap(options, baseEncryptionContext);
                compressor = decryptionContext.getCompressor();
            }
        }
        catch (IOException ioe)
        {
            throw new RuntimeException("unable to load cipher", ioe);
        }
    }

    public static EncryptingCompressor create(Map<String, String> compressionOptions)
    {
        return new EncryptingCompressor(compressionOptions, DatabaseDescriptor.getEncryptionContext());
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        if (encryptCipher == null)
            throw new IllegalStateException("attempting to get the size of an encryption output buffer when this instance will not encrypt");

        int compressedInitialSize = compressor.initialCompressedBufferLength(chunkLength) + EncryptionUtils.COMPRESSED_BLOCK_HEADER_SIZE;
        return encryptCipher.getOutputSize(compressedInitialSize) + EncryptionUtils.ENCRYPTED_BLOCK_HEADER_SIZE;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        // a paranoia check to make sure "we haven't crossed the streams"
        if (encryptCipher == null)
            throw new IllegalStateException("this encryption compression instance has only been created to decrypt, yet it's asked to encrypt");

        if (logger.isTraceEnabled())
            logger.trace("encrypting iv = {} / {}", Arrays.toString(encryptCipher.getIV()), Hex.bytesToHex(encryptCipher.getIV()));

        writeBuffer = EncryptionUtils.compress(input, writeBuffer, true, compressor);
        EncryptionUtils.encrypt(writeBuffer, output, false, encryptCipher);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        ThreadReadState readState = getReadState();
        Cipher cipher = readState.cipher;
        if (logger.isTraceEnabled())
            logger.trace("decrypting iv = {} / {}", Arrays.toString(cipher.getIV()), Hex.bytesToHex(cipher.getIV()));

        readState.readBuffer = EncryptionUtils.decrypt(input, readState.readBuffer, true, cipher);
        EncryptionUtils.uncompress(readState.readBuffer, output, false, compressor);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        ThreadReadState readState = getReadState();
        Cipher cipher = readState.cipher;

        // size the encryption output
        int length = cipher.getOutputSize(inputLength);
        if (readState.readBytes == null || readState.readBytes.length < length)
            readState.readBytes = new byte[length];

        try
        {
            int decryptSize = EncryptionUtils.decrypt(input, inputOffset, readState.readBytes, 0, cipher);
            return EncryptionUtils.uncompress(readState.readBytes, 0, decryptSize, output, outputOffset, compressor);
        }
        catch (BadPaddingException | ShortBufferException | IllegalBlockSizeException e)
        {
            throw new IOException("failed to decrypt block", e);
        }
    }

    public Set<String> supportedOptions()
    {
        return ENCRYPTION_OPTIONS;
    }

    public Map<String, String> compressionParameters()
    {
        return ImmutableMap.of(ENCRYPTION_IV, getIv());
    }

    @VisibleForTesting
    public String getIv()
    {
        byte[] iv = encryptCipher != null ? encryptCipher.getIV() : decryptionContext.getIV();
        return Hex.bytesToHex(iv);
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        return BufferType.ON_HEAP;
    }

    private ThreadReadState getReadState() throws IOException
    {
        ThreadReadState readState = readStates.get();
        if (readState != null)
            return readState;

        readState = new ThreadReadState(decryptionContext.getDecryptor());
        readStates.set(readState);
        return readState;
    }

    /**
     * A simple struct to hold the necessary state for thread-local reads.
     */
    private static final class ThreadReadState
    {
        private final Cipher cipher;
        private ByteBuffer readBuffer;
        private byte[] readBytes;

        private ThreadReadState(Cipher cipher)
        {
            this.cipher = cipher;
        }
    }
}
