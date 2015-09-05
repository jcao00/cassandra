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

import java.net.InetAddress;

/**
 * Callback interface for consumers of the {@code PeerSamplingService}.
 */
public interface PeerSamplingServiceListener
{
    /**
     * Triggered when a node is added to the Peer Sampling Service's active view.
     *
     * @param peer The new peer that was added to the view.
     */
    void neighborUp(InetAddress peer);

    /**
     * Triggered when a node is removed from the Peer Sampling Service's active view.
     *
     * @param peer The peer that was removed the view.
     */
    void neighborDown(InetAddress peer);
}
