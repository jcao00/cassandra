package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

/**
 * A logical, causal clock to be used with the ORSWOT CDRT implementation.
 * I'm not comfortable enough yet with making a generalized clock (like DVV/DVV set or whatnot), so this is a
 * clock based upon the description in the ORSWOT paper, and nothing more general
 * for the time being...
 *
 * Should be used like an immutable clock
 *
 * TODO:JEB think about long-term garbage collection from the clock, as seeds come and go from the cluster
 */
public class OrswotClock
{
    private final Map<InetAddress, Integer> clock;

    public OrswotClock(InetAddress addr)
    {
        clock = ImmutableMap.of(addr, new Integer(0));
    }

    private OrswotClock(Map<InetAddress, Integer> clock)
    {
        this.clock = clock;
    }

    public OrswotClock incrementCounter(InetAddress addr)
    {
        Map<InetAddress, Integer> clockMap = new HashMap<>(clock);
        Integer i = clockMap.get(addr);
        if (i != null)
            clockMap.put(addr, new Integer(i + 1));
        else
            clockMap.put(addr, new Integer(0));

        // not sure I like the copyOf() here, especially since we just copied the entire map a few lines up
        return new OrswotClock(ImmutableMap.copyOf(clockMap));
    }

    public Integer getClockValue(InetAddress address)
    {
        Integer i = clock.get(address);
        if (i != null)
            return i.intValue();
        clock.put(address, new Integer(0));
        return 0;
    }
}
