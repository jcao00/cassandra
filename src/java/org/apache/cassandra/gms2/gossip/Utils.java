package org.apache.cassandra.gms2.gossip;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.io.util.DataOutputPlus;

public class Utils
{
    public static <T> T selectRandom(List<T> source,  final T... toFilter)
    {
        if (source == null || source.isEmpty())
            return null;

        List<T> filtered;
        if (toFilter.length == 0)
        {
            filtered = source;
        }
        else
        {
            filtered = filter(source, toFilter);
        }

        if (filtered.size() == 0)
            return null;

        return filtered.get(ThreadLocalRandom.current().nextInt(0, filtered.size()));
    }

    public static <T> List<T> filter(Collection<T> source, final T... toFilter)
    {
        if (source == null || source.isEmpty())
            return Collections.emptyList();

        return Lists.newArrayList(Iterables.filter(source, new Predicate<T>()
        {
            public boolean apply(T input)
            {
                for (T t : toFilter)
                {
                    if (t.equals(input))
                        return false;
                }
                return true;
            }
        }));
    }

    public static <T> void selectMultipleRandom(Collection<T> source, Collection<T> dest, int maxCount)
    {
        if (dest == null || dest.isEmpty())
            return;
        List<T> copy = new ArrayList<>(source);
        Collections.shuffle(copy);
        int max = Math.min(maxCount, source.size());
        int cnt = 0;
        for (T t : copy)
        {
            if (!dest.contains(t))
            {
                dest.add(t);
                cnt++;
                if (cnt == max)
                    break;
            }
        }
    }

    public static void serialize(InetAddress addr, DataOutputPlus out) throws IOException
    {
        byte[] b = addr.getAddress();
        out.writeByte(b.length);
        out.write(b);
    }

    public static int serializeSize(InetAddress addr)
    {
        // ipAddr len (byte) + ipAddr bytes
        return 1 + addr.getAddress().length;
    }

    public static InetAddress deserialize(DataInput in) throws IOException
    {
        byte[] b = new byte[in.readByte()];
        in.readFully(b);
        return InetAddress.getByAddress(b);
    }
}
