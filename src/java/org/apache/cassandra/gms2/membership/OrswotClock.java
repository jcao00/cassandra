package org.apache.cassandra.gms2.membership;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * A logical, causal clock to be used with the ORSWOT CDRT implementation.
 * I'm not comfortable enough yet with making a generalized clock (like DVV/DVV set or whatnot), so this is a
 * clock based upon the description in the ORSWOT paper, and nothing more general
 * for the time being...
 *
 * Should be used like an immutable clock.
 *
 * TODO:JEB think about long-term garbage collection from the clock, as seeds come and go from the cluster
 *
 * @param <A> The type of the actors of the ORSWOT
 */
public class OrswotClock<A>
{
    private final Map<A, Integer> clock;

    public OrswotClock()
    {
        this(ImmutableMap.<A, Integer>builder().build());
    }

    private OrswotClock(Map<A, Integer> clock)
    {
        this.clock = clock;
    }

    public OrswotClock<A> increment(A a)
    {
        Integer i = clock.get(a);
        return new OrswotClock<>(addOrReplace(a, (i == null ? 1 : i + 1)));
    }

    @VisibleForTesting
    Map<A, Integer> addOrReplace(A a, Integer counter)
    {
        ImmutableMap.Builder<A, Integer> builder = ImmutableMap.<A, Integer>builder();
        builder.put(a, counter);

        for (Map.Entry<A, Integer> entry : clock.entrySet())
        {
            if (!a.equals(entry.getKey()))
                builder.put(entry);
        }

        return builder.build();
    }

    public boolean contains(A a)
    {
        return clock.containsKey(a);
    }

    public Integer getCounter(A a)
    {
        return clock.get(a);
    }

    /**
     * Test is this .....
     *
     * Note: a clock can descend itself, so be careful how the return value is handled
     */
    public boolean descends(OrswotClock<A> clock)
    {
        // TODO: implement me
        return false;
    }

    /**
     * Compare this clock to see if it is "greater than" {@code clock}, but not equal
     */
    public boolean dominates(OrswotClock<A> clock)
    {
        return descends(clock) && !clock.descends(this);
    }

    public OrswotClock merge(OrswotClock<A> clock)
    {
        // TODO: implement me
        return null;
    }

    public String toString()
    {
        return clock.toString();
    }

    @VisibleForTesting
    Map<A, Integer> getClock()
    {
        return clock;
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof OrswotClock))
            return false;
        if (o == this)
            return true;
        OrswotClock<A> orswotClock = (OrswotClock<A>)o;
        return clock.equals(orswotClock.clock);
    }
}
