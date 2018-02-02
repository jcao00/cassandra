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

import lib.llpl.PersistenceException;
import lib.llpl.Transaction;

public class PmemTransaction
{
    private int depth;
    private boolean aborted;

    public void execute(Runnable runnable)
    {
        try
        {
            if (depth == 0)
                Transaction.nativeStartTransaction();
            depth++;
            runnable.run();
        }
        catch(Throwable t)
        {
            aborted = true;
            if (depth == 1)
            {
                // JEB: it's not entirely clear to me why one would only nativeEndTransaction() in the case of a PersistenceException,
                // instead of nativeAbortTransaction() in all cases (shrug)
                if (t instanceof PersistenceException)
                {
                    Transaction.nativeEndTransaction();
                }
                else
                {
                    Transaction.nativeAbortTransaction();
                }
            }
            throw t;
        }
        finally
        {
            depth--;
            if (depth == 0)
            {
                if (!aborted)
                {
                    Transaction.nativeCommitTransaction();
                    Transaction.nativeEndTransaction();
                }
            }
        }
    }
}
