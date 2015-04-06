package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetSocketAddress;
import java.util.List;

public interface HPVConfig
{
    int getShuffleInterval();

    List<InetSocketAddress> getSeeds();

    InetSocketAddress getLocalAddr();

    byte getActiveViewLength();

    byte getPassiveViewLength();

    byte getShuffleActiveViewCount();

    byte getShufflePassiveViewCount();

    byte getShuffleWalkLength();

    byte getActiveRandomWalkLength();

    byte getPassiveRandomWalkLength();
}
