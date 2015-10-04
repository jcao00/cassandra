package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;
import java.util.Optional;

public abstract class HyParViewMessage
{
    public final HPVMessageId messageId;

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
    public final Optional<HPVMessageId> lastDisconnect;

    protected HyParViewMessage(HPVMessageId messageId, InetAddress sender, String datacenter, Optional<HPVMessageId> lastDisconnect)
    {
        this.messageId = messageId;
        this.sender = sender;
        this.datacenter = datacenter;
        this.lastDisconnect = lastDisconnect;
    }

    public abstract HPVMessageType getMessageType();

    public String toString()
    {
        StringBuffer sb = new StringBuffer(128);
        sb.append(getMessageType()).append(", msgId: ").append(messageId);
        sb.append(", from ").append(sender).append(" (").append(datacenter).append(")");
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof HyParViewMessage))
            return false;
        HyParViewMessage msg = (HyParViewMessage)o;

        return messageId.equals(msg.messageId) &&
               sender.equals(msg.sender) &&
               datacenter.equals(msg.datacenter) &&
               getOriginator().equals(msg.getOriginator()) &&
               getOriginatorDatacenter().equals(msg.getOriginatorDatacenter()) &&
               getOriginatorMessageId().equals(msg.getOriginatorMessageId()) &&
               lastDisconnect.equals(msg.lastDisconnect);
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
        return messageId;
    }
}
