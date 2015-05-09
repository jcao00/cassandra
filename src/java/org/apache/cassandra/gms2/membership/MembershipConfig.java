package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.util.Collection;

public interface MembershipConfig
{
    InetAddress getAddress();

    String getDatacenter();

    Collection<InetAddress> getSeeds();
}
