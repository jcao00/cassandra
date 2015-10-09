package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

public class DataMessage extends ThicketMessage
{
    public final Object messageId;
    public final Object payload;
    public final InetAddress sender;
    public final InetAddress originator;

    public DataMessage(InetAddress sender, Object messageId, Object payload, InetAddress originator)
    {
        super();
        this.messageId = messageId;
        this.payload = payload;
        this.sender = sender;
        this.originator = originator;
    }

    public ThicketMessageType getMessageType()
    {
        return ThicketMessageType.DATA;
    }
}
