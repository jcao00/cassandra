package org.apache.cassandra.gossip.thicket;

import java.net.InetAddress;

public class LoadEstimate
{
    final InetAddress treeRoot;
    final int load;

    LoadEstimate(InetAddress treeRoot, int load)
    {
        this.treeRoot = treeRoot;
        this.load = load;
    }

    public String toString()
    {
        return String.format("%s, %s", treeRoot, load);
    }

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof LoadEstimate))
            return false;
        LoadEstimate est = (LoadEstimate) o;
        return treeRoot.equals(est.treeRoot) && load == est.load;
    }

    public int hashCode()
    {
        return treeRoot.hashCode() + (37 * load);
    }
}
