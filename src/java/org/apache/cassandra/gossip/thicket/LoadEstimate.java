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

    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof LoadEstimate))
            return false;
        return treeRoot.equals(((LoadEstimate) o).treeRoot);
    }

    public int hashCode()
    {
        return treeRoot.hashCode();
    }
}
