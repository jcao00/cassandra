package org.apache.cassandra.gossip.hyparview;

import org.junit.Assert;
import org.junit.Test;

public class HPVMessageIdTest
{
    @Test
    public void compareTo_Same()
    {
        HPVMessageId messgeId = new HPVMessageId(42, 9827);
        Assert.assertEquals(0, messgeId.compareTo(messgeId));
    }

    @Test
    public void compareTo_OlderEpoch()
    {
        int epoch = 42;
        HPVMessageId messgeId = new HPVMessageId(epoch, 9827);
        Assert.assertEquals(-1, messgeId.compareTo(new HPVMessageId(epoch + 1, 1)));
    }

    @Test
    public void compareTo_NewerEpoch()
    {
        int epoch = 42;
        HPVMessageId messgeId = new HPVMessageId(epoch, 9827);
        Assert.assertEquals(1, messgeId.compareTo(new HPVMessageId(epoch - 1, 23874273)));
    }

    @Test
    public void compareTo_OlderId()
    {
        int epoch = 23476234;
        int id = 4234102;
        HPVMessageId messgeId = new HPVMessageId(epoch, id);
        Assert.assertEquals(-1, messgeId.compareTo(new HPVMessageId(epoch, id + 1)));
    }

    @Test
    public void compareTo_NewerId()
    {
        int epoch = 23476234;
        int id = 4234102;
        HPVMessageId messgeId = new HPVMessageId(epoch, id);
        Assert.assertEquals(1, messgeId.compareTo(new HPVMessageId(epoch, id - 1)));
    }

    @Test
    public void equals()
    {
        int epoch = 134551;
        int id = 9123;
        HPVMessageId messageId = new HPVMessageId(epoch, id);
        Assert.assertEquals(messageId, new HPVMessageId(epoch, id));
        Assert.assertFalse(messageId.equals(new HPVMessageId(epoch + 1, id)));
        Assert.assertFalse(messageId.equals(new HPVMessageId(epoch, id - 10)));
    }
}
