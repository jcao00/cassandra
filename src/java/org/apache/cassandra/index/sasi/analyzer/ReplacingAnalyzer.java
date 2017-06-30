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

package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ReplacingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(ReplacingAnalyzer.class);
    private ByteBuffer fixedReturn;
    private boolean hasNext = false;

    public void init(Map<String, String> options, AbstractType validator)
    {
        String replace = options.getOrDefault("replace_value", "wtf");
        fixedReturn = ByteBufferUtil.bytes(replace);
        logger.warn("JEB::INIT, replace = {}", replace);
    }

    public boolean hasNext()
    {
        logger.info("JEB::hasNext = {}", hasNext);
        if (hasNext)
        {
            next = fixedReturn.duplicate();
            hasNext = false;
            return true;
        }
        return false;
    }

    public void reset(ByteBuffer input)
    {
        logger.info("JEB::reset()");
        this.next = null;
        this.hasNext = true;
    }
}
