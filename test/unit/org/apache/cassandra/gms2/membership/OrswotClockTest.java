package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrswotClockTest
{
    OrswotClock clock;
    InetAddress addr;

    @Before
    public void setup() throws UnknownHostException
    {
        clock = new OrswotClock();
        addr = InetAddress.getByName("127.0.0.1");
    }

    @Test
    public void increment()
    {
        OrswotClock updatedClock = clock.increment(addr);
        Assert.assertEquals(1, updatedClock.getCounter(addr).intValue());
        updatedClock = updatedClock.increment(addr);
        Assert.assertEquals(2, updatedClock.getCounter(addr).intValue());
    }

    @Test
    public void getCounter_NoEntry()
    {
        Assert.assertNull(clock.getCounter(addr));
    }
}