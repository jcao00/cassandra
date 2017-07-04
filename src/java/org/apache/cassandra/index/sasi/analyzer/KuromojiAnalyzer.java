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
import java.util.Iterator;
import java.util.Map;

import com.atilika.kuromoji.TokenBase;
import com.atilika.kuromoji.TokenizerBase;
import com.atilika.kuromoji.ipadic.Tokenizer;
import org.apache.cassandra.db.marshal.AbstractType;

public class KuromojiAnalyzer extends AbstractAnalyzer
{
    private AbstractType validator;
    private KuromojiAnalyzerOptions options;
    private ByteBuffer input;
    private TokenizerBase tokenizer;
    private Iterator<? extends TokenBase> tokens;

    @Override
    public void init(Map<String, String> options, AbstractType validator)
    {
        init(KuromojiAnalyzerOptions.buildFromMap(options), validator);
    }

    void init(KuromojiAnalyzerOptions tokenizerOptions, AbstractType validator)
    {
        options = tokenizerOptions;
        this.validator = validator;

        if (options.getKuromojiDictionary().equals("ipadic"))
        {
            tokenizer = new Tokenizer();
        }
    }

    @Override
    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.input = input;
        String inString = validator.getString(input);
        tokens = tokenizer.tokenize(inString).iterator();
    }

    @Override
    public boolean hasNext()
    {
        if (tokens.hasNext())
        {
            TokenBase token = tokens.next();
            next = validator.fromString(normalize(token.getSurface()));
            return true;
        }
        return false;
    }
}
