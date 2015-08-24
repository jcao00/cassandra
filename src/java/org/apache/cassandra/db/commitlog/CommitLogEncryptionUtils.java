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
import java.nio.channels.FileChannel;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
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
     * deallocate current, and allocate a large enough buffer.
     * Write the two header lengths (plain text length, compressed length) to the beginning of the buffer as we want those
     * values encapsulated in the encrypted block, as well.
     */
    public static ByteBuffer compress(ByteBuffer inputBuffer, ByteBuffer outputBuffer, boolean allowBufferResize, ICompressor compressor) throws IOException
    {
        int inputLength = inputBuffer.remaining();
        final int compressedLength = compressor.initialCompressedBufferLength(inputLength);
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, compressedLength + COMPRESSED_BLOCK_HEADER_SIZE, allowBufferResize);

        outputBuffer.putInt(inputLength);
        compressor.compress(inputBuffer, outputBuffer);
        outputBuffer.flip();

        return outputBuffer;
    }

    /**
     * Encrypt the input data, and writes out to the same input buffer; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     * Writes the cipher text and headers out to the channel, as well.
     *
     * Note: channel is a parameter as we cannot write header info to the output buffer as we assume the input and output
     * buffers can be the same buffer (and writing the headers to a shared buffer will corrupt any input data). Hence,
     * we write out the headers directly to the channel, and then the cipher text (once encrypted).
     */
    public static ByteBuffer encrypt(ByteBuffer inputBuffer, FileChannel channel, boolean allowBufferResize, Cipher cipher) throws IOException
    {
        final int plainTextLength = inputBuffer.remaining();
        final int encryptLength = cipher.getOutputSize(plainTextLength);
        ByteBuffer outputBuffer = inputBuffer.duplicate();
        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, encryptLength, allowBufferResize);

        // it's unfortunate that we need to allocate a small buffer here just for the headers, but that's all channel gives us :-/
        ByteBuffer intBuf = ByteBuffer.allocate(8);
        intBuf.putInt(0, encryptLength);
        intBuf.putInt(4, plainTextLength);
        channel.write(intBuf);

        try
        {
            cipher.doFinal(inputBuffer, outputBuffer);
        }
        catch (ShortBufferException | IllegalBlockSizeException | BadPaddingException e)
        {
            throw new IOException("failed to encrypt commit log block", e);
        }

        outputBuffer.position(0).limit(encryptLength);
        channel.write(outputBuffer);
        outputBuffer.position(0).limit(encryptLength);

        return outputBuffer;
    }

    /**
     * Decrypt the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
     */
    public static ByteBuffer decrypt(FileDataInput dataInput, ByteBuffer outputBuffer, boolean allowBufferResize, Cipher cipher) throws IOException, BadPaddingException, ShortBufferException, IllegalBlockSizeException
    {
        int encryptedLength = dataInput.readInt();
        // this is the length of the compressed data
        int plainTextLength = dataInput.readInt();

        outputBuffer = ByteBufferUtil.ensureCapacity(outputBuffer, Math.max(plainTextLength, encryptedLength), allowBufferResize);
        // we should only be performing encrypt/decrypt operations with on-heap buffers, so calling BB.array() should be legit here
        dataInput.readFully(outputBuffer.array(), 0, encryptedLength);

        ByteBuffer dupe = outputBuffer.duplicate();
        dupe.clear();
        cipher.doFinal(outputBuffer, dupe);
        dupe.position(0).limit(plainTextLength);

        return dupe;
    }

    /**
     * Uncompress the input data, as well as manage sizing of the {@code outputBuffer}; if the buffer is not big enough,
     * deallocate current, and allocate a large enough buffer.
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
