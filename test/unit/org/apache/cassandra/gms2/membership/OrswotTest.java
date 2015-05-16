package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrswotTest
{
    Orswot<InetAddress, InetAddress> orswot;
    InetAddress localAddr, remoteSeed, addr1, addr2, addr3;

    @Before
    public void setup() throws UnknownHostException
    {
        localAddr = InetAddress.getByName("127.0.0.0");
        remoteSeed = InetAddress.getByName("127.60.0.0");
        addr1 = InetAddress.getByName("127.0.0.1");
        addr2 = InetAddress.getByName("127.0.0.2");
        addr3 = InetAddress.getByName("127.0.0.3");
        orswot = new Orswot<>(localAddr);
    }

    @Test
    public void add_SameNodeMultipleTimes()
    {
        orswot.add(addr1);
        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        Map<InetAddress, Integer> clock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(1, clock.get(localAddr).intValue());

        orswot.add(addr1);
        orswot.add(addr1);

        // make sure is still in orswot, and counter = 3
        currentState = orswot.getCurrentState();
        clock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(3, clock.get(localAddr).intValue());

        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(1, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertEquals(3, masterClock.get(localAddr).intValue());
    }

    Orswot.TaggedElement<InetAddress, InetAddress> getElement(Orswot.SetAndClock<InetAddress, InetAddress> currentState, InetAddress target)
    {
        for (Orswot.TaggedElement<InetAddress, InetAddress> element : currentState.elements)
        {
            if (element.t.equals(target))
                return element;
        }
        return null;
    }

    @Test
    public void add_SameNodeMultipleTimes_DifferentSeeds()
    {
        orswot.add(addr1, localAddr);
        orswot.add(addr1, remoteSeed);
        orswot.add(addr1, localAddr);

        // make sure is in orswot, and counter = 3
        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        Map<InetAddress, Integer> clock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(2, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(2, clock.get(localAddr).intValue());
        Assert.assertEquals(1, clock.get(remoteSeed).intValue());

        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(2, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertEquals(2, masterClock.get(localAddr).intValue());
        Assert.assertEquals(1, masterClock.get(remoteSeed).intValue());
    }

    @Test
    public void add_MultipleNodes()
    {
        orswot.add(addr1);
        orswot.add(addr2);
        orswot.add(addr3);
        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        // make sure each node is in orswot, and counter is as each step
        // make sure master clock = 3 for localAddr

        Map<InetAddress, Integer> clock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(1, clock.get(localAddr).intValue());

        clock = getElement(currentState, addr2).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(2, clock.get(localAddr).intValue());

        clock = getElement(currentState, addr3).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(3, clock.get(localAddr).intValue());

        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(1, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertEquals(3, masterClock.get(localAddr).intValue());
    }

    @Test
    public void add_MultipleNodes_DifferentSeeds()
    {
        orswot.add(addr1, localAddr);
        orswot.add(addr2, remoteSeed);
        orswot.add(addr3, remoteSeed);
        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();

        // check the master clock for the entire set has both updating nodes in it
        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(2, masterClock.size());
        Assert.assertEquals(1, masterClock.get(localAddr).intValue());
        Assert.assertEquals(2, masterClock.get(remoteSeed).intValue());


        // make sure each node is in orswot, and counter is as each step
        // make sure master clock = 3 for localAddr

        Map<InetAddress, Integer> clock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertFalse(clock.containsKey(remoteSeed));
        Assert.assertEquals(1, clock.get(localAddr).intValue());

        clock = getElement(currentState, addr2).clock.getClock();
        Assert.assertEquals(2, clock.size());
        Assert.assertTrue(clock.containsKey(remoteSeed));
        Assert.assertEquals(1, clock.get(remoteSeed).intValue());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(1, clock.get(localAddr).intValue());

        clock = getElement(currentState, addr3).clock.getClock();
        Assert.assertEquals(2, clock.size());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(2, clock.get(remoteSeed).intValue());
        Assert.assertTrue(clock.containsKey(localAddr));
        Assert.assertEquals(1, clock.get(localAddr).intValue());
    }

    @Test
    public void remove_EmptySet()
    {
        Assert.assertNull(orswot.remove(addr2));
    }

    @Test
    public void remove_MultiElementSet()
    {
        OrswotClock<InetAddress> addr1Clock = orswot.add(addr1, localAddr);
        OrswotClock<InetAddress> addr2Clock = orswot.add(addr2, remoteSeed);
        OrswotClock<InetAddress> addr3Clock = orswot.add(addr3, remoteSeed);

        Orswot.SetAndClock<InetAddress, InetAddress> stateBeforeRemove = orswot.getCurrentState();
        Assert.assertEquals(3, stateBeforeRemove.elements.size());

        // remove an element
        Assert.assertEquals(addr2Clock, orswot.remove(addr2));
        Orswot.SetAndClock<InetAddress, InetAddress> stateAfterRemove = orswot.getCurrentState();
        Assert.assertEquals(2, stateAfterRemove.elements.size());
        Assert.assertEquals(stateBeforeRemove.clock, stateAfterRemove.clock);

        // remove another element
        Assert.assertEquals(addr1Clock, orswot.remove(addr1));
        stateAfterRemove = orswot.getCurrentState();
        Assert.assertEquals(1, stateAfterRemove.elements.size());
        Assert.assertEquals(stateBeforeRemove.clock, stateAfterRemove.clock);

        // remove last element
        Assert.assertEquals(addr3Clock, orswot.remove(addr3));
        stateAfterRemove = orswot.getCurrentState();
        Assert.assertEquals(0, stateAfterRemove.elements.size());
        Assert.assertEquals(stateBeforeRemove.clock, stateAfterRemove.clock);
    }

    @Test
    public void applyAdd_EmptySet()
    {
        Map<InetAddress, Integer> clock = new HashMap<>();
        clock.put(localAddr, 1);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(clock);

        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(1, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertEquals(1, masterClock.get(localAddr).intValue());

        Map<InetAddress, Integer> elementClock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, elementClock.size());
        Assert.assertTrue(elementClock.containsKey(localAddr));
        Assert.assertEquals(1, elementClock.get(localAddr).intValue());
    }

    @Test
    public void applyAdd_ExistingEntryWithLowerClock()
    {
        Map<InetAddress, Integer> clock = new HashMap<>();
        clock.put(localAddr, 1);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(clock);
        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        OrswotClock<InetAddress> incrementedClock = orswotClock.increment(localAddr);
        Assert.assertTrue(orswot.applyAdd(addr1, incrementedClock));

        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(1, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertEquals(masterClock.get(localAddr).toString(), 2, masterClock.get(localAddr).intValue());

        Map<InetAddress, Integer> elementClock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(1, elementClock.size());
        Assert.assertTrue(elementClock.containsKey(localAddr));
        Assert.assertEquals(2, elementClock.get(localAddr).intValue());
    }

    @Test
    public void applyAdd_ExistingEntryWithGreaterClock()
    {
        Map<InetAddress, Integer> clock = new HashMap<>();
        clock.put(localAddr, 2);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(clock);
        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        clock = new HashMap<>();
        clock.put(localAddr, 1);
        orswotClock = new OrswotClock<>(clock);
        Assert.assertFalse(orswot.applyAdd(addr1, orswotClock));
        // make sure master clock has not changed
        Assert.assertEquals(orswotClock, orswot.getCurrentState().clock);
    }

    @Test
    public void applyAdd_DisjointClocks()
    {
        Map<InetAddress, Integer> clock = new HashMap<>();
        clock.put(localAddr, 2);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(clock);
        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        clock = new HashMap<>();
        clock.put(localAddr, 1);
        clock.put(remoteSeed, 3);
        OrswotClock<InetAddress> newClock = new OrswotClock<>(clock);
        Assert.assertFalse(orswot.applyAdd(addr1, newClock));
        // make sure master clock has not changed
        Assert.assertEquals(orswotClock, orswot.getCurrentState().clock);
    }

    @Test
    public void applyAdd_ClockWithNewEntry()
    {
        Map<InetAddress, Integer> clock = new HashMap<>();
        clock.put(localAddr, 2);
        OrswotClock<InetAddress> orswotClock = new OrswotClock<>(clock);
        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        clock = new HashMap<>();
        clock.put(localAddr, 2);
        clock.put(remoteSeed, 1);
        orswotClock = new OrswotClock<>(clock);
        Assert.assertTrue(orswot.applyAdd(addr1, orswotClock));

        Orswot.SetAndClock<InetAddress, InetAddress> currentState = orswot.getCurrentState();
        Map<InetAddress, Integer> masterClock = currentState.clock.getClock();
        Assert.assertEquals(2, masterClock.size());
        Assert.assertTrue(masterClock.containsKey(localAddr));
        Assert.assertTrue(masterClock.containsKey(remoteSeed));
        Assert.assertEquals(masterClock.get(localAddr).toString(), 2, masterClock.get(localAddr).intValue());
        Assert.assertEquals(masterClock.get(remoteSeed).toString(), 1, masterClock.get(remoteSeed).intValue());

        Map<InetAddress, Integer> elementClock = getElement(currentState, addr1).clock.getClock();
        Assert.assertEquals(2, elementClock.size());
        Assert.assertTrue(elementClock.containsKey(localAddr));
        Assert.assertEquals(2, elementClock.get(localAddr).intValue());
        Assert.assertTrue(elementClock.containsKey(remoteSeed));
        Assert.assertEquals(1, elementClock.get(remoteSeed).intValue());
    }
}
