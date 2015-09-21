package org.apache.cassandra.gossip.hyparview;

import org.junit.Assert;
import org.junit.Test;

public class HPVMessageIdTest
{
    @Test
    public void compareTo_Same()
    {
        HPVMessageId messgeId = new HPVMessageId(42L, 9827);
        Assert.assertEquals(0, messgeId.compareTo(messgeId));
    }

    @Test
    public void compareTo_OlderEpoch()
    {
        long epoch = 42;
        HPVMessageId messgeId = new HPVMessageId(epoch, 9827);
        Assert.assertEquals(-1, messgeId.compareTo(new HPVMessageId(epoch + 1, 1)));
    }

    @Test
    public void compareTo_NewerEpoch()
    {
        long epoch = 42;
        HPVMessageId messgeId = new HPVMessageId(epoch, 9827);
        Assert.assertEquals(1, messgeId.compareTo(new HPVMessageId(epoch - 1, 23874273)));
    }

    @Test
    public void compareTo_OlderId()
    {
        long epoch = 23476234L;
        int id = 4234102;
        HPVMessageId messgeId = new HPVMessageId(epoch, id);
        Assert.assertEquals(-1, messgeId.compareTo(new HPVMessageId(epoch, id + 1)));
    }

    @Test
    public void compareTo_NewerId()
    {
        long epoch = 23476234L;
        int id = 4234102;
        HPVMessageId messgeId = new HPVMessageId(epoch, id);
        Assert.assertEquals(1, messgeId.compareTo(new HPVMessageId(epoch, id - 1)));
    }
}
