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

package org.apache.cassandra.db;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.utils.FBUtilities;

public interface MemtableFactory
{
    Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, ColumnFamilyStore cfs);

    public static MemtableFactory createInstance(String className)
    {
        if (className == null || className.isEmpty())
            throw new ConfigurationException("Need a valid memtable factory name; was null or empty");

        className = className.contains(".") ? className : "org.apache.cassandra.db." + className;

        try
        {
            return FBUtilities.instanceOrConstruct(className, "memtable factory class");
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("%s is not found or cannot be instantiated (%s)", TableParams.Option.MEMTABLE_FACTORY, className));
        }
    }
}
