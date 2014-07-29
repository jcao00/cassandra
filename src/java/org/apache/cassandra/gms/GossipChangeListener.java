package org.apache.cassandra.gms;

import com.google.common.collect.Multimap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.ISO_8859_1;

public class GossipChangeListener implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(GossipChangeListener.class);
    private final TokenMetadata tokenMetadata;
    private final PendingRangeCalculatorService rangeCalculator;
    private final Gossiper gossiper;
    private final IPartitioner partitioner;

    public GossipChangeListener(TokenMetadata tokenMetadata, PendingRangeCalculatorService rangeCalculator, Gossiper gossiper, IPartitioner partitioner)
    {
        this.tokenMetadata = tokenMetadata;
        this.rangeCalculator = rangeCalculator;
        this.gossiper = gossiper;
        this.partitioner = partitioner;
    }

    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet())
        {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state.equals(ApplicationState.STATUS))
        {
            String apStateValue = value.value;
            String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName)
            {
                case VersionedValue.STATUS_BOOTSTRAPPING:
                    handleStateBootstrap(endpoint);
                    break;
                case VersionedValue.STATUS_NORMAL:
                    handleStateNormal(endpoint);
                    break;
                case VersionedValue.REMOVING_TOKEN:
                case VersionedValue.REMOVED_TOKEN:
                    handleStateRemoving(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_LEAVING:
                    handleStateLeaving(endpoint);
                    break;
                case VersionedValue.STATUS_LEFT:
                    handleStateLeft(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_MOVING:
                    handleStateMoving(endpoint, pieces);
                    break;
                case VersionedValue.STATUS_RELOCATING:
                    handleStateRelocating(endpoint, pieces);
                    break;
            }
        }
    }

    /**
     * Handle node bootstrap
     *
     * @param endpoint bootstrapping node
     */
    private void handleStateBootstrap(InetAddress endpoint)
    {
        // explicitly check for TOKENS, because a bootstrapping node might be bootstrapping in legacy mode; that is, not using vnodes and no token specified
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state bootstrapping, token {}", endpoint, tokens);

        // if this node is present in token metadata, either we have missed intermediate states
        // or the node had crashed. Print warning if needed, clear obsolete stuff and
        // continue.
        if (tokenMetadata.isMember(endpoint))
        {
            // If isLeaving is false, we have missed both LEAVING and LEFT. However, if
            // isLeaving is true, we have only missed LEFT. Waiting time between completing
            // leave operation and rebootstrapping is relatively short, so the latter is quite
            // common (not enough time for gossip to spread). Therefore we report only the
            // former in the log.
            if (!tokenMetadata.isLeaving(endpoint))
                logger.info("Node {} state jump to bootstrap", endpoint);
            tokenMetadata.removeEndpoint(endpoint);
        }

        tokenMetadata.addBootstrapTokens(tokens, endpoint);
        rangeCalculator.update(tokenMetadata);

        if (gossiper.usesHostId(endpoint))
            tokenMetadata.updateHostId(gossiper.getHostId(endpoint), endpoint);
    }

    private Collection<Token> getTokensFor(InetAddress endpoint)
    {
        try
        {
            String vvalue = gossiper.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.TOKENS).value;
            byte[] bytes = vvalue.getBytes(ISO_8859_1);
            return TokenSerializer.deserialize(partitioner, new DataInputStream(new ByteArrayInputStream(bytes)));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }


    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(InetAddress endpoint)
    {
        gossiper.removeEndpoint(endpoint);
    }

    protected void addExpireTimeIfFound(InetAddress endpoint, long expireTime)
    {
        if (expireTime != 0L)
        {
            gossiper.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }


    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final InetAddress endpoint)
    {
        Collection<Token> tokens = getTokensFor(endpoint);

        Set<Token> tokensToUpdateInMetadata = new HashSet<>();
        Set<InetAddress> endpointsToRemove = new HashSet<>();

        if (logger.isDebugEnabled())
            logger.debug("Node {} state normal, token {}", endpoint, tokens);

        if (tokenMetadata.isMember(endpoint))
            logger.info("Node {} state jump to normal", endpoint);

        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see CASSANDRA-4300).
        if (gossiper.usesHostId(endpoint))
        {
            UUID hostId = gossiper.getHostId(endpoint);
            InetAddress existing = tokenMetadata.getEndpointForHostId(hostId);
            if (DatabaseDescriptor.isReplacing() && gossiper.getEndpointStateForEndpoint(DatabaseDescriptor.getReplaceAddress()) != null && (hostId.equals(gossiper.getHostId(DatabaseDescriptor.getReplaceAddress()))))
                logger.warn("Not updating token metadata for {} because I am replacing it", endpoint);
            else
            {
                if (existing != null && !existing.equals(endpoint))
                {
                    if (existing.equals(gossiper.broadcastAddr))
                    {
                        logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                        tokenMetadata.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                    else if (gossiper.compareEndpointStartup(endpoint, existing) > 0)
                    {
                        logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing, endpoint, endpoint);
                        tokenMetadata.removeEndpoint(existing);
                        endpointsToRemove.add(existing);
                        tokenMetadata.updateHostId(hostId, endpoint);
                    }
                    else
                    {
                        logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint, endpoint);
                        tokenMetadata.removeEndpoint(endpoint);
                        endpointsToRemove.add(endpoint);
                    }
                }
                else
                    tokenMetadata.updateHostId(hostId, endpoint);
            }
        }

        for (final Token token : tokens)
        {
            // we don't want to update if this node is responsible for the token and it has a later startup time than endpoint.
            InetAddress currentOwner = tokenMetadata.getEndpoint(token);
            if (currentOwner == null)
            {
                logger.debug("New node {} at token {}", endpoint, token);
                tokensToUpdateInMetadata.add(token);
            }
            else if (endpoint.equals(currentOwner))
            {
                // set state back to normal, since the node may have tried to leave, but failed and is now back up
                tokensToUpdateInMetadata.add(token);
            }
            else if (tokenMetadata.isRelocating(token) && tokenMetadata.getRelocatingRanges().get(token).equals(endpoint))
            {
                // Token was relocating, this is the bookkeeping that makes it official.
                tokensToUpdateInMetadata.add(token);

                StorageService.optionalTasks.schedule(new Runnable()
            {
                public void run()
                {
                    logger.info("Removing RELOCATION state for {} {}", endpoint, token);
                    tokenMetadata.removeFromRelocating(token, endpoint);
                }
            }, StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
                logger.info("Token {} relocated to {}", token, endpoint);
            }
            else if (tokenMetadata.isRelocating(token))
            {
                logger.info("Token {} is relocating to {}, ignoring update from {}",
                        token, tokenMetadata.getRelocatingRanges().get(token), endpoint);
            }
            else if (gossiper.compareEndpointStartup(endpoint, currentOwner) > 0)
            {
                tokensToUpdateInMetadata.add(token);

                // currentOwner is no longer current, endpoint is.  Keep track of these moves, because when
                // a host no longer has any tokens, we'll want to remove it.
                Multimap<InetAddress, Token> epToTokenCopy = tokenMetadata.getEndpointToTokenMapForReading();
                epToTokenCopy.get(currentOwner).remove(token);
                if (epToTokenCopy.get(currentOwner).size() < 1)
                    endpointsToRemove.add(currentOwner);

                logger.info(String.format("Nodes %s and %s have the same token %s.  %s is the new owner",
                        endpoint,
                        currentOwner,
                        token,
                        endpoint));
                if (logger.isDebugEnabled())
                    logger.debug("Relocating ranges: {}", tokenMetadata.printRelocatingRanges());
            }
            else
            {
                logger.info(String.format("Nodes %s and %s have the same token %s.  Ignoring %s",
                        endpoint,
                        currentOwner,
                        token,
                        endpoint));
                if (logger.isDebugEnabled())
                    logger.debug("Relocating ranges: {}", tokenMetadata.printRelocatingRanges());
            }
        }

        tokenMetadata.updateNormalTokens(tokensToUpdateInMetadata, endpoint);
        for (InetAddress ep : endpointsToRemove)
            removeEndpoint(ep);

        if (tokenMetadata.isMoving(endpoint)) // if endpoint was moving to a new token
        {
            tokenMetadata.removeFromMoving(endpoint);
        }

        rangeCalculator.update(tokenMetadata);
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(InetAddress endpoint, String[] pieces)
    {
        assert (pieces.length > 0);
        if (tokenMetadata.isMember(endpoint))
        {
            String state = pieces[0];
            Collection<Token> removeTokens = tokenMetadata.getTokens(endpoint);

            if (VersionedValue.REMOVED_TOKEN.equals(state))
            {
                excise(removeTokens, endpoint);
            }
            else if (VersionedValue.REMOVING_TOKEN.equals(state))
            {
                if (logger.isDebugEnabled())
                    logger.debug("Tokens {} removed manually (endpoint was {})", removeTokens, endpoint);

                // Note that the endpoint is being removed
                tokenMetadata.addLeavingEndpoint(endpoint);
                rangeCalculator.update(tokenMetadata);
            }
        }
        else // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
        {
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }


    protected long extractExpireTime(String[] pieces)
    {
        return Long.parseLong(pieces[2]);
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(InetAddress endpoint)
    {
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state leaving, tokens {}", endpoint, tokens);

        // If the node is previously unknown or tokens do not match, update tokenmetadata to
        // have this node as 'normal' (it must have been using this token before the
        // leave). This way we'll get pending ranges right.
        if (!tokenMetadata.isMember(endpoint))
        {
            logger.info("Node {} state jump to leaving", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }
        else if (!tokenMetadata.getTokens(endpoint).containsAll(tokens))
        {
            logger.warn("Node {} 'leaving' token mismatch. Long network partition?", endpoint);
            tokenMetadata.updateNormalTokens(tokens, endpoint);
        }

        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        tokenMetadata.addLeavingEndpoint(endpoint);
        rangeCalculator.update(tokenMetadata);
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Collection<Token> tokens = getTokensFor(endpoint);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state left, tokens {}", endpoint, tokens);

        addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
        excise(tokens, endpoint);
    }

    private void excise(Collection<Token> tokens, InetAddress endpoint)
    {
        logger.info("Removing tokens {} for {}", tokens, endpoint);
        removeEndpoint(endpoint);
        tokenMetadata.removeEndpoint(endpoint);
        tokenMetadata.removeBootstrapTokens(tokens);
        rangeCalculator.update(tokenMetadata);
    }

    /**
     * Handle node moving inside the ring.
     *
     * @param endpoint moving endpoint address
     * @param pieces STATE_MOVING, token
     */
    private void handleStateMoving(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;
        Token token = partitioner.getTokenFactory().fromString(pieces[1]);

        if (logger.isDebugEnabled())
            logger.debug("Node {} state moving, new token {}", endpoint, token);

        tokenMetadata.addMovingEndpoint(token, endpoint);
        rangeCalculator.update(tokenMetadata);
    }

    /**
     * Handle one or more ranges (tokens) moving from their respective endpoints, to another.
     *
     * @param endpoint the destination of the move
     * @param pieces STATE_RELOCATING,token,token,...
     */
    private void handleStateRelocating(InetAddress endpoint, String[] pieces)
    {
        assert pieces.length >= 2;

        List<Token> tokens = new ArrayList<>(pieces.length - 1);
        for (String tStr : Arrays.copyOfRange(pieces, 1, pieces.length))
            tokens.add(partitioner.getTokenFactory().fromString(tStr));

        logger.debug("Tokens {} are relocating to {}", tokens, endpoint);
        tokenMetadata.addRelocatingTokens(tokens, endpoint);

        rangeCalculator.update(tokenMetadata);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
    }

    public void onRemove(InetAddress endpoint)
    {
        tokenMetadata.removeEndpoint(endpoint);
        rangeCalculator.update(tokenMetadata);
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
    }

}
