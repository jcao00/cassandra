package org.apache.cassandra.gossip.hyparview;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.gossip.GossipMessageId;

public class GossipMessageIdTest
{
    @Test
    public void compareTo_Same()
    {
        GossipMessageId messgeId = new GossipMessageId(42, 9827);
        Assert.assertEquals(0, messgeId.compareTo(messgeId));
    }

    @Test
    public void compareTo_OlderEpoch()
    {
        int epoch = 42;
        GossipMessageId messgeId = new GossipMessageId(epoch, 9827);
        Assert.assertEquals(-1, messgeId.compareTo(new GossipMessageId(epoch + 1, 1)));
    }

    @Test
    public void compareTo_NewerEpoch()
    {
        int epoch = 42;
        GossipMessageId messgeId = new GossipMessageId(epoch, 9827);
        Assert.assertEquals(1, messgeId.compareTo(new GossipMessageId(epoch - 1, 23874273)));
    }

    @Test
    public void compareTo_OlderId()
    {
        int epoch = 23476234;
        int id = 4234102;
        GossipMessageId messgeId = new GossipMessageId(epoch, id);
        Assert.assertEquals(-1, messgeId.compareTo(new GossipMessageId(epoch, id + 1)));
    }

    @Test
    public void compareTo_NewerId()
    {
        int epoch = 23476234;
        int id = 4234102;
        GossipMessageId messgeId = new GossipMessageId(epoch, id);
        Assert.assertEquals(1, messgeId.compareTo(new GossipMessageId(epoch, id - 1)));
    }

    @Test
    public void equals()
    {
        int epoch = 134551;
        int id = 9123;
        GossipMessageId messageId = new GossipMessageId(epoch, id);
        Assert.assertEquals(messageId, new GossipMessageId(epoch, id));
        Assert.assertFalse(messageId.equals(new GossipMessageId(epoch + 1, id)));
        Assert.assertFalse(messageId.equals(new GossipMessageId(epoch, id - 10)));
    }
}
