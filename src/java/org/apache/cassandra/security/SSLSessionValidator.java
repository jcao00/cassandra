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

package org.apache.cassandra.security;

import java.util.Map;

import io.netty.channel.Channel;

/**
 * Expects implementors to have a constructor that takes {@link Map <String, String>} as the sole parameter.
 */
public interface SSLSessionValidator
{
    /**
     * Validate the ssl session related to the channel is legit.
     *
     * NOTE: This function should not block! Making network requests is a bad thing here as you'll block
     * the netty io thread. Reading from disk is probably not a hot idea, either, but that might be unavoidable.
     */
    boolean validate(Channel channel);
}
