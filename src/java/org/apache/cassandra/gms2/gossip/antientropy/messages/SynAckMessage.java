package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public class SynAckMessage extends AntiEntropyMessage
{
    private final Object pushPullData;

    public SynAckMessage(String clientId, Object pushPullData)
    {
        super(clientId);
        this.pushPullData = pushPullData;
    }

    public Object getPushPullData()
    {
        return pushPullData;
    }

    public MessageType getMessageType()
    {
        return MessageType.SYN_ACK;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
