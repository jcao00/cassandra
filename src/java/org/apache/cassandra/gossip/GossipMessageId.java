package org.apache.cassandra.gossip;

import java.util.concurrent.atomic.AtomicInteger;

public class GossipMessageId implements Comparable<GossipMessageId>
{
    private final int epoch;
    private final int id;

    public GossipMessageId(int epoch, int id)
    {
        this.epoch = epoch;
        this.id = id;
    }

    public int compareTo(GossipMessageId other)
    {
        if (epoch == other.epoch)
        {
            if (id == other.id)
                return 0;
            return id < other.id ? -1 : 1;
        }

        return epoch < other.epoch ? -1 : 1;
    }

    public int epochOnlyCompareTo(GossipMessageId other)
    {
        return epoch == other.epoch ? 0 : epoch < other.epoch ? -1 : 1;
    }

    public int getEpoch()
    {
        return epoch;
    }

    public int getId()
    {
        return id;
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof GossipMessageId))
            return false;
        GossipMessageId msgId = (GossipMessageId)o;
        return epoch == msgId.epoch && id == msgId.id;
    }

    public String toString()
    {
        return epoch + ":" + id;
    }

    public static class IdGenerator
    {
        private final AtomicInteger id = new AtomicInteger();
        private final int epoch;

        public IdGenerator(int epoch)
        {
            this.epoch = epoch;
        }

        public GossipMessageId generate()
        {
            return new GossipMessageId(epoch, id.getAndIncrement());
        }
    }
}
