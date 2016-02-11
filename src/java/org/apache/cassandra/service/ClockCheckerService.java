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

package org.apache.cassandra.service;

import java.lang.management.ManagementFactory;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.metrics.ClockCheckerMetrics;

/**
 * A basic service that checks the system clock for major shifts in time.
 */
public class ClockCheckerService
{
    private static final Logger logger = LoggerFactory.getLogger(ClockCheckerService.class);
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=ClockChecker";

    enum ClockStatus { LEGIT, BACKWARD, TOO_FAR_AHEAD, NOT_FAST_ENOUGH }

    static final String INTERVAL_PROPERTY = "cassandra.clockCheck.interval";

    /**
     * The minimum number of allowable milliseconds between clock checks.
     */
    static final long MIN_INTERVAL_MILLIS = 1000;
    static final long DEFAULT_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(300);

    static final long INTERVAL_MILLIS = deriveInterval();
    static final long ALLOWABLE_OFFSET_SIZE_MILLIS = calculateAllowableOffset();

    // a singleton instance to be used as a convenience to the rest of the app,
    // but is not a requirement
    public static final ClockCheckerService instance = new ClockCheckerService();

    private final ScheduledExecutorService executor = new DebuggableScheduledThreadPoolExecutor(1, "ClockChecker", Thread.NORM_PRIORITY);
    private final ClockCheckerMetrics metrics;
    private volatile boolean started;

    /**
     * Figure out the time interval in milliseconds between clock checks.
     */
    @VisibleForTesting
    static long deriveInterval()
    {
        String intervalSetting = System.getProperty(INTERVAL_PROPERTY, String.valueOf(DEFAULT_INTERVAL_MILLIS));
        try
        {
            long val = Long.parseLong(intervalSetting);
            if (val < MIN_INTERVAL_MILLIS)
            {
                logger.warn("value for property {} is too low ({}), will use the base minimum instead:", INTERVAL_PROPERTY, val, MIN_INTERVAL_MILLIS);
                return MIN_INTERVAL_MILLIS;
            }
            return val;
        }
        catch (NumberFormatException nfe)
        {
            logger.warn("improperly encoded value for property {}: {}. defaulting to {}", INTERVAL_PROPERTY, intervalSetting, DEFAULT_INTERVAL_MILLIS);
            return DEFAULT_INTERVAL_MILLIS;
        }
    }

    private static long calculateAllowableOffset()
    {
        // defaulting a wiggle room as twice the interval distance, but give it a base minumum time between readings
        // in case of long GC pauses or something else horrible
        return Math.max(INTERVAL_MILLIS * 2, TimeUnit.SECONDS.toMillis(300));
    }

    ClockCheckerService()
    {
        metrics = new ClockCheckerMetrics();
    }

    public void start()
    {
        if (started)
            return;
        started = true;
        executor.scheduleWithFixedDelay(new ClockChecker(System.currentTimeMillis()), INTERVAL_MILLIS, INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    class ClockChecker implements Runnable
    {
        private volatile long lastRecordedTime;

        public ClockChecker(long initialTime)
        {
            lastRecordedTime = initialTime;
        }

        public void run()
        {
            try
            {
                checkClock();
            }
            catch (Exception e)
            {
                logger.error("failed to calculate current (wall-clock) time", e);
            }
        }

        ClockStatus checkClock()
        {
            final long previousTime = lastRecordedTime;
            final long currentTime = System.currentTimeMillis();
            lastRecordedTime = currentTime;

            if (currentTime < previousTime)
            {
                logger.error("current clock reading is less than the previously recorded value (clock moved backward): {} vs. {}", currentTime, previousTime);
                metrics.clockBackwardCount.inc();
                return ClockStatus.BACKWARD;
            }

            long diff = currentTime - previousTime;
            if (diff > ALLOWABLE_OFFSET_SIZE_MILLIS)
            {
                logger.error("current time reading is further ahead than expected (clock jumped ahead): {} vs. {} with a wiggle room of {}", currentTime, previousTime, ALLOWABLE_OFFSET_SIZE_MILLIS);
                metrics.clockAheadCount.inc();
                return ClockStatus.TOO_FAR_AHEAD;
            }
            // check to see if the clock moved * at least* the number of millis between each execution
            else if (diff < INTERVAL_MILLIS)
            {
                logger.error("current time reading is behind where it should be (clock did not move ahead as expected): {} vs. {}", currentTime, (previousTime + INTERVAL_MILLIS));
                metrics.clockLaggingCount.inc();
                return ClockStatus.NOT_FAST_ENOUGH;
            }

            return ClockStatus.LEGIT;
        }
    }
}
