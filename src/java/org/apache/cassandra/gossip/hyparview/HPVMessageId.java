package org.apache.cassandra.gossip.hyparview;

import java.util.concurrent.atomic.AtomicInteger;

public class HPVMessageId implements Comparable<HPVMessageId>
{
    private final long epoch;
    private final int id;

    public HPVMessageId(long epoch, int id)
    {
        this.epoch = epoch;
        this.id = id;
    }

    public int compareTo(HPVMessageId other)
    {
        if (epoch == other.epoch)
        {
            if (id == other.id)
                return 0;
            return id < other.id ? -1 : 1;
        }

        return epoch < other.epoch ? -1 : 1;
    }

    public int epochOnlyCompareTo(HPVMessageId other)
    {
        return epoch == other.epoch ? 0 : epoch < other.epoch ? -1 : 1;
    }

    public String toString()
    {
        return epoch + ":" + id;
    }

    public static class IdGenerator
    {
        private final AtomicInteger id = new AtomicInteger();
        private final long epoch;

        public IdGenerator(long epoch)
        {
            this.epoch = epoch;
        }

        public HPVMessageId generate()
        {
            return new HPVMessageId(epoch, id.getAndIncrement());
        }
    }
}
