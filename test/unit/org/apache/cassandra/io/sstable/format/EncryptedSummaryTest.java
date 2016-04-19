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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.security.CipherFactoryTest;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.tools.ant.taskdefs.Sync;

public class EncryptedSummaryTest
{
    @Test
    public void roundTrip() throws IOException
    {
        File f = File.createTempFile("enc-summary-" + UUID.randomUUID(), ".db");
        Map<String, String> options = new HashMap<>();

        EncryptionContext context = EncryptionContextGenerator.createContext(false);
        options.put(EncryptionContext.ENCRYPTION_KEY_ALIAS, context.getTransparentDataEncryptionOptions().key_alias);
        options.put(EncryptionContext.ENCRYPTION_CIPHER, context.getTransparentDataEncryptionOptions().cipher);

        ByteBuffer buf = ByteBufferUtil.bytes(CipherFactoryTest.ULYSSEUS);
        try (DataOutputStreamPlus outputStream = new BufferedDataOutputStreamPlus(
                                                     new EncryptedSummaryWritableByteChannel(
                                                         new FileOutputStream(f), options, context)))
        {
            ByteBufferUtil.bytes(CipherFactoryTest.ULYSSEUS);
            outputStream.write(buf);
        }

        ByteBuffer readBuf = ByteBuffer.allocate(buf.capacity());
        try (DataInputStream inputStream = new DataInputStream(
                                              new EncryptedSummaryInputStream(
                                                  new FileInputStream(f), options, context)))
        {
            inputStream.read(readBuf.array());
            Assert.assertEquals(buf, readBuf);
            Assert.assertEquals(0, inputStream.available());
        }
    }
}
