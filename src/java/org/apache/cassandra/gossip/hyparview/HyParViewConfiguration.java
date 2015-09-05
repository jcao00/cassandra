package org.apache.cassandra.gossip.hyparview;

import org.apache.cassandra.locator.SeedProvider;

public interface HyParViewConfiguration
{
    int activeRandomWalkLength();

    SeedProvider seedProvider();
}

