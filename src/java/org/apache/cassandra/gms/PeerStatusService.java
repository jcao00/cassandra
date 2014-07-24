package org.apache.cassandra.gms;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.net.InetAddress;

public class PeerStatusService
{
    public final Gossiper gossiper;
    public final FailureDetector fd;
    public final TokenMetadata tokenMetadata;
    public final PendingRangeCalculatorService rangeCalculator;
    public final VersionedValue.VersionedValueFactory versionedValueFactory;

    private final GossipChangeListener gossipChangeListener;

    public PeerStatusService(IPartitioner partitioner, boolean registerJmx)
    {
        gossiper = new Gossiper(registerJmx);
        versionedValueFactory = new VersionedValue.VersionedValueFactory(partitioner, gossiper.versionGenerator);
        fd = new FailureDetector(gossiper, registerJmx);
        tokenMetadata = new TokenMetadata(fd);
        rangeCalculator = new PendingRangeCalculatorService(registerJmx);

        gossipChangeListener = new GossipChangeListener(tokenMetadata, rangeCalculator, gossiper, partitioner);
        gossiper.register(gossipChangeListener);
    }

    public boolean isAlive()
    {
        return true;
    }

    public boolean isMember(InetAddress endpoint)
    {
        return tokenMetadata.isMember(endpoint);
    }

    public void startLeaving()
    {
        gossiper.addLocalApplicationState(ApplicationState.STATUS, versionedValueFactory.leaving(StorageService.instance.getLocalTokens()));
        tokenMetadata.addLeavingEndpoint(FBUtilities.getBroadcastAddress());
        rangeCalculator.update(tokenMetadata);
    }

    public void leaveRing()
    {
        tokenMetadata.removeEndpoint(FBUtilities.getBroadcastAddress());
        rangeCalculator.update(tokenMetadata);
        gossiper.addLocalApplicationState(ApplicationState.STATUS, versionedValueFactory.left(StorageService.instance.getLocalTokens(), Gossiper.computeExpireTime()));
    }
}
