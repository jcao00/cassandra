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
package org.apache.cassandra.gossip;

import org.apache.cassandra.io.IVersionedSerializer;

/**
 * Acts as a callback, via {@link #receive(Object)}, for {@link BroadcastService}s when messages are received from peers.
 */
public interface BroadcastServiceClient<T>
{
    /**
     * A unique name for this client.
     */
    String getClientName();

    /**
     * Handle a broadcasted message.
     *
     * @return true if the has not been seen before (not delivered earlier);
     * else, false if it is a duplicate.
     */
    boolean receive(T payload);

    IVersionedSerializer<T> getSerializer();
}
