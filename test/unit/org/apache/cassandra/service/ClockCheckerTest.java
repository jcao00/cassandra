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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.service.ClockCheckerService.ClockChecker;

public class ClockCheckerTest
{
    @Test
    public void deriveInterval_GoodValue()
    {
        long interval = ClockCheckerService.DEFAULT_INTERVAL_MILLIS + 27;
        System.setProperty(ClockCheckerService.INTERVAL_PROPERTY, String.valueOf(interval));
        long derived = ClockCheckerService.deriveInterval();
        Assert.assertEquals(interval, derived);
    }

    @Test
    public void deriveInterval_GarbageValue()
    {
        System.setProperty(ClockCheckerService.INTERVAL_PROPERTY, "skjdgfaygfsnfvlb");
        long derived = ClockCheckerService.deriveInterval();
        Assert.assertEquals(ClockCheckerService.DEFAULT_INTERVAL_MILLIS, derived);
    }

    @Test
    public void deriveInterval_SmallValue()
    {
        long interval = ClockCheckerService.MIN_INTERVAL_MILLIS - 1;
        System.setProperty(ClockCheckerService.INTERVAL_PROPERTY, String.valueOf(interval));
        long derived = ClockCheckerService.deriveInterval();
        Assert.assertEquals(ClockCheckerService.MIN_INTERVAL_MILLIS, derived);
    }

    @Test
    public void checkClock_HappyPath()
    {
        ClockChecker checker = ClockCheckerService.instance.new ClockChecker(System.currentTimeMillis() - (ClockCheckerService.ALLOWABLE_OFFSET_SIZE_MILLIS - 10));
        Assert.assertEquals(ClockCheckerService.ClockStatus.LEGIT, checker.checkClock());
    }

    @Test
    public void checkClock_ClockNotMovingForwardFastEnough()
    {
        ClockChecker checker = ClockCheckerService.instance.new ClockChecker(System.currentTimeMillis() - 10);
        Assert.assertEquals(ClockCheckerService.ClockStatus.NOT_FAST_ENOUGH, checker.checkClock());
    }

    @Test
    public void checkClock_ClockMovedBackward()
    {
        ClockChecker checker = ClockCheckerService.instance.new ClockChecker(System.currentTimeMillis() + 10);
        Assert.assertEquals(ClockCheckerService.ClockStatus.BACKWARD, checker.checkClock());
    }

    @Test
    public void checkClock_ClockJumpedAhead()
    {
        ClockChecker checker = ClockCheckerService.instance.new ClockChecker(System.currentTimeMillis() - (ClockCheckerService.ALLOWABLE_OFFSET_SIZE_MILLIS * 2));
        Assert.assertEquals(ClockCheckerService.ClockStatus.TOO_FAR_AHEAD, checker.checkClock());
    }
}
