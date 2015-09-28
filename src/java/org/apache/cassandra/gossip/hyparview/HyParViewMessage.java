package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Map;

public abstract class HyParViewMessage
{
    public final HPVMessageId messgeId;

    /**
     * Address of the peer that is sending/sent this message.
     */
    public final InetAddress sender;

    /**
     * Datacenter of the sender.
     */
    public final String datacenter;

    /**
     * The message id of the last disconnect the sender has received from the recipient.
     */
    public final HPVMessageId lastDisconnect;

    protected HyParViewMessage(HPVMessageId messgeId, InetAddress sender, String datacenter, HPVMessageId lastDisconnect)
    {
        this.messgeId = messgeId;
        this.sender = sender;
        this.datacenter = datacenter;
        this.lastDisconnect = lastDisconnect;
    }

    public abstract HPVMessageType getMessageType();

    public String toString()
    {
        StringBuffer sb = new StringBuffer(128);
        sb.append(getMessageType()).append(", msgId: ").append(messgeId);
        sb.append(", from ").append(sender).append(" (").append(datacenter).append(")");
        return sb.toString();
    }

    public InetAddress getOriginator()
    {
        return sender;
    }

    public String getOriginatorDatacenter()
    {
        return datacenter;
    }

    public HPVMessageId getOriginatorMessageId()
    {
        return messgeId;
    }

    public HPVMessageId getLastDisconnect()
    {
        return lastDisconnect;
    }

}
