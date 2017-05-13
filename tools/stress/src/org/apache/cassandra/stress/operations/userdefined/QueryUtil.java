package org.apache.cassandra.stress.operations.userdefined;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import com.datastax.driver.core.PreparedStatement;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;

import java.util.List;

public class QueryUtil
{
    public static enum ArgSelect
    {
        MULTIROW, SAMEROW;
        //TODO: FIRSTROW, LASTROW
    }

    public static boolean dynamicConditionExists(PreparedStatement statement) throws IllegalArgumentException
    {
        if (statement.getQueryString().toUpperCase().startsWith("UPDATE"))
        {
            ModificationStatement.Parsed modificationStatement = null;
            try
            {
                modificationStatement = CQLFragmentParser.parseAnyUnhandled(CqlParser::updateStatement,
                        statement.getQueryString());
            }
            catch (RecognitionException e)
            {
                e.printStackTrace();
                throw new IllegalArgumentException("could not parse update query:" + statement.getQueryString());
            }

            List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> casConditionList = modificationStatement.getConditions();
            if (casConditionList.size() > 0)
            {
                /** here we differentiate between static condition vs dynamic condition
                 *  static condition example: if col1 = NULL
                 *  dynamic condition example: if col1 = ?
                 *  for static condition we don't have to replace value, no extra work
                 *  involved
                 *  for dynamic condition we have to read existing db value and then
                 *  use current db values during the update
                 */
                boolean conditionExpectsDynamicValue = false;
                for (Pair<ColumnMetadata.Raw, ColumnCondition.Raw> condition : casConditionList)
                {
                    /**if condition is like a = 10 then condition.right has string value "NULL"
                     * if condition is like a = ? then condition.right has string value "?"
                     * so compare value "NULL" and see if this is a static condition or dynamic
                     */
                    if (condition.right.getValue().getText().equals("?"))
                    {
                        conditionExpectsDynamicValue = true;
                        break;
                    }
                }
                if (!conditionExpectsDynamicValue)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        }
        return false;
    }
}
