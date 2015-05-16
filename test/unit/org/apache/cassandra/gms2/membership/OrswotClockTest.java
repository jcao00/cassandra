package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrswotClockTest
{
    OrswotClock<InetAddress> clock;
    InetAddress addr, addr2, addr3;

    @Before
    public void setup() throws UnknownHostException
    {
        clock = new OrswotClock<>();
        addr = InetAddress.getByName("127.0.0.1");
        addr2 = InetAddress.getByName("127.0.0.2");
        addr3 = InetAddress.getByName("127.0.0.3");
    }

    @Test
    public void increment()
    {
        OrswotClock<InetAddress> updatedClock = clock.increment(addr);
        Assert.assertEquals(1, updatedClock.getCounter(addr).intValue());
        updatedClock = updatedClock.increment(addr);
        Assert.assertEquals(2, updatedClock.getCounter(addr).intValue());
    }

    @Test
    public void getCounter_NoEntry()
    {
        Assert.assertNull(clock.getCounter(addr));
    }

    @Test
    public void descends_OtherIsEmpty()
    {
        Assert.assertTrue(clock.descends(new OrswotClock<>()));
    }

    @Test
    public void descends_ThisIsEmpty()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(otherClock);
        Assert.assertFalse(clock.descends(orswotClock));
    }

    @Test
    public void descends_ContainsMoreEntries()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>(otherClock);
        otherClock.put(addr2, 4);
        otherClock.put(addr3, 1);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertTrue(clock2.descends(clock1));
    }

    @Test
    public void descends_DifferentEntries()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        otherClock.put(addr3, 1);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        otherClock.put(addr2, 4);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertFalse(clock2.descends(clock1));
    }

    @Test
    public void descends_OldClock()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>();
        otherClock.put(addr, 2);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertFalse(clock2.descends(clock1));
    }

    @Test
    public void dominates_EmptyLists()
    {
        OrswotClock<InetAddress> clock1 = new OrswotClock<>();
        OrswotClock<InetAddress> clock2 = new OrswotClock<>();
        Assert.assertFalse(clock1.dominates(clock2));
    }

    @Test
    public void dominates_EmptyListAndPopulatedOther()
    {
        OrswotClock<InetAddress> clock1 = new OrswotClock<>();
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);
        Assert.assertFalse(clock1.dominates(clock2));
    }

    @Test
    public void dominates_PopulatedListAndEmptyOther()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>();
        Assert.assertTrue(clock1.dominates(clock2));
    }

    @Test
    public void dominates_PopulatedTheSame()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertFalse(clock1.dominates(clock2));
    }

    @Test
    public void dominates_PopulatedDifferent_Lower()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        otherClock.put(addr2, 2);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertFalse(clock1.dominates(clock2));
    }

    @Test
    public void dominates_PopulatedDifferent_Higher()
    {
        Map<InetAddress, Integer> otherClock = new HashMap<>();
        otherClock.put(addr, 7);
        otherClock.put(addr2, 2);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(otherClock);

        otherClock = new HashMap<>();
        otherClock.put(addr, 5);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(otherClock);

        Assert.assertTrue(clock1.dominates(clock2));
    }
}