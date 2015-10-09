package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

public class SummaryMessage extends ThicketMessage
{
    public SummaryMessage(InetAddress sender)
    {

    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.SUMMARY;
    }
}
