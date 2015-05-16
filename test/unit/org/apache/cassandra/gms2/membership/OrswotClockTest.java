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

    @Test
    public void merge_EmptyLists()
    {
        OrswotClock<InetAddress> clock1 = new OrswotClock<>();
        OrswotClock<InetAddress> clock2 = new OrswotClock<>();
        Assert.assertTrue(clock1.merge(clock2).getClock().isEmpty());
    }

    @Test
    public void merge_OneEmptyList()
    {
        Map<InetAddress, Integer> map = new HashMap<>();
        map.put(addr, 7);
        map.put(addr2, 2);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(map);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>();
        OrswotClock<InetAddress> result = clock1.merge(clock2);
        Assert.assertTrue(result.equals(clock1));
        Assert.assertFalse(result.equals(clock2));
    }

    @Test
    public void merge_TwoDifferentLists()
    {
        Map<InetAddress, Integer> map1 = new HashMap<>();
        map1.put(addr, 7);
        map1.put(addr2, 2);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(map1);

        Map<InetAddress, Integer> map2 = new HashMap<>();
        map2.put(addr3, 1);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(map2);

        OrswotClock<InetAddress> result = clock1.merge(clock2);
        Assert.assertFalse(result.equals(clock1));
        Assert.assertTrue(result.descends(clock1));
        Assert.assertFalse(result.equals(clock2));
        Assert.assertTrue(result.descends(clock2));

        for (Map.Entry<InetAddress, Integer> entry : clock1.getClock().entrySet())
            Assert.assertEquals(entry.getValue(), result.getCounter(entry.getKey()));
        for (Map.Entry<InetAddress, Integer> entry : clock2.getClock().entrySet())
            Assert.assertEquals(entry.getValue(), result.getCounter(entry.getKey()));
    }

    @Test
    public void merge_OverlappingLists()
    {
        Map<InetAddress, Integer> map1 = new HashMap<>();
        map1.put(addr, 7);
        map1.put(addr2, 2);
        OrswotClock<InetAddress> clock1 = new OrswotClock<>(map1);

        Map<InetAddress, Integer> map2 = new HashMap<>();
        map2.put(addr, 3);
        map2.put(addr3, 1);
        OrswotClock<InetAddress> clock2 = new OrswotClock<>(map2);

        OrswotClock<InetAddress> result = clock1.merge(clock2);
        Assert.assertFalse(result.equals(clock1));
        Assert.assertTrue(result.descends(clock1));
        Assert.assertFalse(result.equals(clock2));
        Assert.assertTrue(result.descends(clock2));

        Assert.assertEquals(7, result.getCounter(addr).intValue());
        Assert.assertEquals(2, result.getCounter(addr2).intValue());
        Assert.assertEquals(1, result.getCounter(addr3).intValue());
    }
}