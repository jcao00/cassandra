package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;

import org.apache.cassandra.io.ISerializer;

public class ThicketDataMessage extends ThicketMessage
{
    private final String clientId;
    private final Object messageId;
    private final Object message;

    public ThicketDataMessage(InetAddress treeRoot, String clientId, Object messageId, Object message, byte[] loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.clientId = clientId;
        this.messageId = messageId;
        this.message = message;
    }

    public ThicketDataMessage(ThicketDataMessage msg, byte[] loadEstimate)
    {
        this(msg.getTreeRoot(), msg.getClientId(), msg.getMessageId(), msg.getMessage(), loadEstimate);
    }

    public String getClientId()
    {
        return clientId;
    }

    public Object getMessageId()
    {
        return messageId;
    }

    public Object getMessage()
    {
        return message;
    }

    public MessageType getMessageType()
    {
        return MessageType.DATA;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
