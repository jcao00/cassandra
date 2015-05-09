package org.apache.cassandra.gms2.gossip.antientropy.messages;

import org.apache.cassandra.io.ISerializer;

public abstract class AntiEntropyMessage
{
    private final String clientId;

    protected AntiEntropyMessage(String clientId)
    {
        this.clientId = clientId;
    }

    public String getClientId()
    {
        return clientId;
    }

    public abstract MessageType getMessageType();

    abstract ISerializer getSerializer();
}
