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
import java.util.Map;
import java.util.Set;

/**
 * It is expected that implementing classes also implement a static creation method, as well:
 *
 * public static ICompressor create(Map<String, String> compressionOptions)
 */
public interface ICompressor
{
    int initialCompressedBufferLength(int chunkLength);

    int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

    /**
     * Compression for ByteBuffers.
     *
     * The data between input.position() and input.limit() is compressed and placed into output starting from output.position().
     * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
     */
    void compress(ByteBuffer input, ByteBuffer output) throws IOException;

    /**
     * Decompression for DirectByteBuffers.
     *
     * The data between input.position() and input.limit() is uncompressed and placed into output starting from output.position().
     * Positions in both buffers are moved to reflect the bytes read and written. Limits are not changed.
     */
    void uncompress(ByteBuffer input, ByteBuffer output) throws IOException;

    /**
     * Returns the preferred (most efficient) buffer type for this compressor.
     */
    BufferType preferredBufferType();

    /**
     * Checks if the given buffer would be supported by the compressor. If a type is supported the compressor must be
     * able to use it in combination with all other supported types.
     *
     * Direct and memory-mapped buffers must be supported by all compressors.
     */
    boolean supports(BufferType bufferType);

    Set<String> supportedOptions();

    /**
     * Retrieve instance-specific parameters for this compressor.
     * @return custom map of name-value pairs; else, an empty map.
     */
    Map<String, String> compressionParameters();
}
