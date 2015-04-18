package org.apache.cassandra.gms2.gossip.thicket.messages;

import java.net.InetAddress;

import org.apache.cassandra.io.ISerializer;

/**
 * Choosing to create a GRAFT response message rather than overload the PRUNE message. T
 * he intended functionality is the same, but the separation makes the code easier to comprehend and cleaner to implement.
 */
public class GraftResponseMessage extends ThicketMessage
{
    public enum State {ACCEPT, REJECT}

    private final InetAddress treeRoot;
    private final State state;

    public GraftResponseMessage(InetAddress treeRoot, State state, float loadEstimate)
    {
        super(treeRoot, loadEstimate);
        this.treeRoot = treeRoot;
        this.state = state;
    }

    public InetAddress getTreeRoot()
    {
        return treeRoot;
    }

    public State getState()
    {
        return state;
    }

    public MessageType getMessageType()
    {
        return MessageType.GRAFT_RESPONSE;
    }

    public ISerializer getSerializer()
    {
        return null;
    }
}
