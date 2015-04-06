package org.apache.cassandra.gms2.gossip;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple class for generating a global monotonic sequence. Can be used as the basis for
 * <a href="http://www.cs.cornell.edu/home/rvr/papers/flowgossip.pdf">Scuttlebutt</a>-style versioning.
 */
public class VersionGenerator
{
    private static final AtomicLong sequencer = new AtomicLong(0);

    public static long getNextValue()
    {
        return sequencer.getAndDecrement();
    }
}
