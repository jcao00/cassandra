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

package org.apache.cassandra.streaming;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.Config;

/**
 * A mechanism to rate limit the amount of data to process for a streaming activity.
 * The {@link RateLimiter}s limits are tiered:  first you have to aquire the global permits from the {@link #limiter},
 * then if the destination in not in the same datacenter, you need to acquire the {@link #interDCLimiter} permits.
 *
 * When any of the streaming throughput values in {@link Config} is 0, the corresponding rate limiter
 * is set to {@link #DEFAULT_RATE_LIMIT}. Rate unit is bytes per sec.
 */
public class StreamRateLimiter
{
    private static final Double DEFAULT_RATE_LIMIT = Double.MAX_VALUE;

    /**
     * A global singleton instance intended for most application purposes.
     */
    public static final StreamRateLimiter instance = new StreamRateLimiter();

    private static final double BYTES_PER_MEGABIT = (1024 * 1024) / 8; // from bits

    /**
     * A global rate limit on streaming data to all peers in a cluster.
     */
    private final RateLimiter limiter;

    /**
     * A global rate limit on streaming data to all non-local datacenters.
     */
    private final RateLimiter interDCLimiter;

    private StreamRateLimiter()
    {
        this (DEFAULT_RATE_LIMIT, DEFAULT_RATE_LIMIT);
    }

    @VisibleForTesting
    public StreamRateLimiter(double globalLimit, double interDcLimit)
    {
        limiter = RateLimiter.create(globalLimit);
        interDCLimiter =  RateLimiter.create(interDcLimit);
    }

    /**
     * Update the global streaming throughput to all peers.
     *
     * @param limit throughput limit, in MB.
     */
    public void updateGlobalThroughput(double limit)
    {
        maybeUpdateThroughput(limit, limiter);
    }

    private void maybeUpdateThroughput(double limit, RateLimiter rateLimiter)
    {
        // if throughput is set to 0, throttling is disabled
        if (limit <= 0)
            limit = DEFAULT_RATE_LIMIT;
        else
            limit *= BYTES_PER_MEGABIT;

        if (rateLimiter.getRate() != limit)
            rateLimiter.setRate(limit);
    }

    /**
     * Update the global streaming throughput to all remote datacenters.
     *
     * @param limit throughput limit, in MB.
     */
    public void updateInterDcThroughput(double limit)
    {
        maybeUpdateThroughput(limit, interDCLimiter);
    }

    public boolean tryAcquireGlobal(int toTransfer)
    {
        // good news: RateLimiter allows us to attempt to acquire without blocking.
        // bad news: if we can't acquire, the RateLimiter API does not tell us how long we should wait before trying again :(
        return limiter.tryAcquire(toTransfer);
    }

    public boolean tryAcquireInterDc(int toTransfer)
    {
        // good news: RateLimiter allows us to attempt to acquire without blocking.
        // bad news: if we can't acquire, the RateLimiter API does not tell us how long we should wait before trying again :(
        return interDCLimiter.tryAcquire(toTransfer);
    }
}
