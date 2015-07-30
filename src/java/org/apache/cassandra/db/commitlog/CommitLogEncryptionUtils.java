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
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Encryption and decryption functions specific to the commit log.
 * See comments in {@link EncryptedSegment} for details on the binary format.
 * The normal, and expected, invocation pattern is to compress then encrypt the data on the encryption pass,
 * then decrypt and uncompress the data on the decrypt pass.
 */
public class CommitLogEncryptionUtils
{
    public static final int COMPRESSED_BLOCK_HEADER_SIZE = 4;
    public static final int ENCRYPTED_BLOCK_HEADER_SIZE = 8;

    /**
     * Compress the raw data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer (and stash back into the thread local buffer holder).
     * Write the two header lengths (plain text length, compressed length) to the beginning of the buffer as we want those
     * values encapsulated in the encrypted block, as well.
     */
    public static ByteBuffer compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException
    {
        ByteBuffer inputBufferSlice = inputBuffer.duplicate();
        int inputLength = inputBuffer.remaining();
        final int compressedLength = compressor.initialCompressedBufferLength(inputLength);
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, compressedLength + COMPRESSED_BLOCK_HEADER_SIZE, allowBufferResize);

        outputBuffer.putInt(inputLength);
        compressor.compress(inputBufferSlice, outputBuffer);
        outputBuffer.flip();

        return outputBuffer;
    }

    /**
     * Encrypt the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer (and stash back into the thread local buffer holder).
     */
    public static ByteBuffer encrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        final int plainTextLength = inputBuffer.remaining();
        final int encryptLength = cipher.getOutputSize(plainTextLength);
        final int outputLength = encryptLength + ENCRYPTED_BLOCK_HEADER_SIZE;
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, outputLength, allowBufferResize);
        outputBuffer.putInt(encryptLength);
        outputBuffer.putInt(plainTextLength);

        try
        {
            cipher.doFinal(inputBuffer, outputBuffer);
        }
        catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("failed to encrypt commit log block", e);
        }

        outputBuffer.position(0).limit(outputLength);

        return outputBuffer;
    }

    /**
     * Decrypt the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer (and stash back into the thread local buffer holder).
     */
    public static ByteBuffer decrypt(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws BadPaddingException, ShortBufferException, IllegalBlockSizeException
    {
        ByteBuffer inBuf = inputBuffer.duplicate();
        int encryptedLength = inBuf.getInt();
        int plainTextLength = inBuf.getInt();

        inBuf.limit(inBuf.position() + encryptedLength);
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, Math.max(plainTextLength, encryptedLength), allowBufferResize);

        cipher.doFinal(inBuf, outputBuffer);
        outputBuffer.position(0).limit(plainTextLength);
        inputBuffer.position(inBuf.position());

        return outputBuffer;
    }

    /**
     * Uncompress the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer (and stash back into the thread local buffer holder).
     */
    public static ByteBuffer uncompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException
    {
        int outputLength = inputBuffer.getInt();
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, outputLength, allowBufferResize);
        compressor.uncompress(inputBuffer, outputBuffer);
        outputBuffer.position(0).limit(outputLength);

        return outputBuffer;
    }
}
