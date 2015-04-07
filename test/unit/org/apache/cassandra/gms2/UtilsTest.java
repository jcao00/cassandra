package org.apache.cassandra.gms2;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.io.util.DataOutputBuffer;

public class UtilsTest
{
    List<String> simpsons;

    @Before
    public void setUp()
    {
        simpsons = new ArrayList<String>()
        {{
            add("homer");
            add("marge");
            add("bart");
            add("lisa");
            add("maggie");
        }};
    }

    @Test
    public void filterEmptyNull()
    {
        Assert.assertTrue(Utils.filter(null).isEmpty());
        Assert.assertTrue(Utils.filter(new ArrayList<>()).isEmpty());
    }

    @Test
    public void filterNonEmpty()
    {
        List<String> kids = Utils.filter(new ArrayList<>(simpsons), "homer", "marge");
        Assert.assertTrue(kids.contains("bart"));
        Assert.assertTrue(kids.contains("lisa"));
        Assert.assertTrue(kids.contains("maggie"));
        Assert.assertFalse(kids.contains("homer"));
        Assert.assertFalse(kids.contains("marge"));
    }

    @Test
    public void selectRandomEmptyNull()
    {
        Assert.assertNull(Utils.selectRandom(null));
        Assert.assertNull(Utils.selectRandom(new ArrayList<String>()));
    }

    @Test
    public void selectRandomNoFilter()
    {
        selectRandomTest();
    }

    private void selectRandomTest(String... toFilter)
    {
        int maxCount = 1024;
        HashMap<String, Integer> map = new HashMap<>(simpsons.size());
        for (int i = 0; i < maxCount; i++)
        {
            String s = Utils.selectRandom(simpsons, toFilter);
            if (s == null)
            {
                Assert.assertTrue(simpsons.size() == toFilter.length);
                continue;
            }
            Assert.assertTrue(simpsons.contains(s));
            if (map.containsKey(s))
                map.put(s, map.get(s).intValue() + 1);
            else
                map.put(s, 0);
        }

        for (String s : simpsons)
        {
            boolean isFiltered = false;
            for (String filter : toFilter)
            {
                if (filter.equals(s))
                {
                    isFiltered = true;
                    break;
                }
            }

            if (isFiltered)
            {
                Assert.assertFalse(map.containsKey(s));
                continue;
            }
            Assert.assertTrue(map.containsKey(s));
            int cnt = map.get(s);

            //Note: not expecting uniform distribution form the random selection, but it *should* be greater than zero
            Assert.assertTrue(0 < cnt);
        }
    }

    @Test
    public void selectRandomFilterOne()
    {
        selectRandomTest(simpsons.get(0));
    }

    @Test
    public void selectRandomFilterTwo()
    {
        selectRandomTest(simpsons.get(3), simpsons.get(1));
    }

    @Test
    public void selectRandomFilterAll()
    {
        selectRandomTest(simpsons.toArray(new String[0]));
    }

    @Test
    public void inetSocketAddressSeriailization() throws IOException
    {
        InetAddress addr = InetAddress.getByName("127.0.0.1");
        DataOutputBuffer buf = new DataOutputBuffer();
        int expectedLen = Utils.serializeSize(addr);
        Utils.serialize(addr, buf);
        Assert.assertEquals(expectedLen, buf.getLength());
        DataInput in = ByteStreams.newDataInput(buf.getData());
        InetAddress ret = Utils.deserialize(in);
        Assert.assertEquals(addr, ret);
    }

    @Test
    public void selectMultipleRandomEmpty()
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        Utils.selectMultipleRandom(source, dest, 1);
        Assert.assertTrue(dest.isEmpty());
    }

    @Test
    public void selectMultipleRandomEmptySourceWithOne() throws UnknownHostException
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        dest.add(InetAddress.getByName("127.0.0.1"));
        Utils.selectMultipleRandom(source, dest, 1);
        Assert.assertEquals(1, dest.size());
    }

    @Test
    public void selectMultipleRandomEmptyMerge() throws UnknownHostException
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        dest.add(InetAddress.getByName("127.0.0.1"));
        Utils.selectMultipleRandom(source, dest, 1);
        Assert.assertEquals(1, dest.size());
    }

    @Test
    public void selectMultipleRandomMergeNoDupes() throws UnknownHostException
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        dest.add(InetAddress.getByName("127.0.0.1"));
        source.add(InetAddress.getByName("127.0.0.2"));
        Utils.selectMultipleRandom(source, dest, 1);
        Assert.assertEquals(2, dest.size());
    }

    @Test
    public void selectMultipleRandomMergeDupe() throws UnknownHostException
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        dest.add(InetAddress.getByName("127.0.0.5"));
        source.add(InetAddress.getByName("127.0.0.5"));
        Utils.selectMultipleRandom(source, dest, 3);
        Assert.assertEquals(1, dest.size());
    }

    @Test
    public void selectMultipleRandomMergeFull() throws UnknownHostException
    {
        Collection<InetAddress> source = new ArrayList<>();
        Collection<InetAddress> dest = new ArrayList<>();
        dest.add(InetAddress.getByName("127.0.0.1"));
        dest.add(InetAddress.getByName("127.0.0.5"));
        dest.add(InetAddress.getByName("127.0.0.6"));

        for (int i = 7; i >= 0; i--)
            source.add(InetAddress.getByName("127.0.0." + i));
        Utils.selectMultipleRandom(source, dest, 3);
        Assert.assertEquals(6, dest.size());
    }
}
