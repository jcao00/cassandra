package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.util.List;

public interface HPVConfig
{
    int getShuffleInterval();

    List<InetAddress> getSeeds();

    InetAddress getLocalAddr();

    byte getActiveViewLength();

    byte getPassiveViewLength();

    byte getShuffleActiveViewCount();

    byte getShufflePassiveViewCount();

    byte getShuffleWalkLength();

    byte getActiveRandomWalkLength();

    byte getPassiveRandomWalkLength();
}
