package org.apache.cassandra.gossip.hyparview;

import java.net.InetAddress;

public abstract class HyParViewMessage
{
    public final InetAddress requestor;
    public final String datacenter;

    public HyParViewMessage(InetAddress requestor, String datacenter)
    {
        this.requestor = requestor;
        this.datacenter = datacenter;
    }

    public abstract HPVMessageType getMessageType();
}
