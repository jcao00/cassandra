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

package org.apache.cassandra.db.pmem;

import java.io.IOException;

import lib.llpl.MemoryRegion;
import org.apache.cassandra.io.util.DataInputPlus;

public class MemoryRegionDataInputPlus implements DataInputPlus
{
    private final MemoryRegion region;
    private final long size;
    private int position;

    public MemoryRegionDataInputPlus(MemoryRegion region)
    {
        this.region = region;
    }

    public long position()
    {
        return position;
    }

    public void position(int position)
    {
        this.position = position;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {

    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {

    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        return 0;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return false;
    }

    @Override
    public byte readByte() throws IOException
    {
        return 0;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return 0;
    }

    @Override
    public short readShort() throws IOException
    {
        return 0;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return 0;
    }

    @Override
    public char readChar() throws IOException
    {
        return 0;
    }

    @Override
    public int readInt() throws IOException
    {
        return 0;
    }

    @Override
    public long readLong() throws IOException
    {
        return 0;
    }

    @Override
    public float readFloat() throws IOException
    {
        return 0;
    }

    @Override
    public double readDouble() throws IOException
    {
        return 0;
    }

    @Override
    public String readLine() throws IOException
    {
        return null;
    }

    @Override
    public String readUTF() throws IOException
    {
        return null;
    }
}
