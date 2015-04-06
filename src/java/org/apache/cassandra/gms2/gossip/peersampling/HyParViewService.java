package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms2.gossip.GossipBroadcaster;
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.peersampling.messages.*;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborRequest.Priority;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborResponse.Result;

/**
 * An implementation of the HyParView peer sampling service (http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf).
 *
 * Note: this class is intentionally not thread safe, and assumes a single threaded execution model.
 * This is to keep the implementation simple, and, really, this data/functionality should not be a hot spot.
 */
public class HyParViewService implements PeerSamplingService
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewService.class);
    private final List<InetSocketAddress> activeView;
    private final List<InetSocketAddress> passiveView;
    private final HPVConfig config;
    private final GossipDispatcher dispatcher;

    private final Set<GossipBroadcaster> broadcasters;

    public HyParViewService(HPVConfig config, GossipDispatcher dispatcher)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        activeView = new CopyOnWriteArrayList<>();
        passiveView = new CopyOnWriteArrayList<>();
        broadcasters = new CopyOnWriteArraySet<>();
    }

    public void init(ScheduledExecutorService scheduledService, IFailureDetector failureDetector)
    {
        failureDetector.registerFailureDetectionEventListener(new FailureDetectorListener(this));
        join();

        scheduledService.scheduleAtFixedRate(new ShuffleRoundStarter(), 60,
                                             config.getShuffleInterval(), TimeUnit.SECONDS);
    }

    private static class FailureDetectorListener implements IFailureDetectionEventListener
    {
        // TODO: get from yaml ... maybe??
        private static final int DEFAULT_PEER_PORT = 9090;
        private final HyParViewService service;

        private FailureDetectorListener(HyParViewService service)
        {
            this.service = service;
        }

        public void convict(InetAddress ep, double phi)
        {
            service.handlePeerFailure(new InetSocketAddress(ep, DEFAULT_PEER_PORT));
        }
    }

    public void join()
    {
        InetSocketAddress seed = Utils.selectRandom(config.getSeeds());

        if (seed == null)
        {
            logger.info("no unique seed addresses available to send join request to");
            return;
        }

        // TODO: add callback to ensure we got a response (Neighbor) msg from some peer
        dispatcher.send(this, new Join(), seed);
    }

    public void handle(HyParViewMessage msg, InetSocketAddress sender)
    {
        switch (msg.getMessageType())
        {
            case JOIN:              handleJoin((Join)msg, sender); break;
            case FORWARD_JOIN:      handleForwardJoin((ForwardJoin)msg, sender); break;
            case JOIN_ACK:          handleJoinACk((JoinAck)msg, sender); break;
            case DISCONNECT:        handleDisconnect((Disconnect)msg, sender); break;
            case NEIGHBOR_REQUEST:  handleNeighborRequest((NeighborRequest)msg, sender); break;
            case NEIGHBOR_RESPONSE: handleNeighborResponse((NeighborResponse)msg, sender); break;
            case SHUFFLE:           handleShuffle((Shuffle)msg, sender); break;
            case SHUFFLE_RESPONSE:  handleShuffleReply((ShuffleResponse)msg, sender); break;
            default:
                throw new IllegalArgumentException("Unexpected HyParView message type: " + msg.getMessageType());
        }
    }

    public void handleJoin(Join msg, InetSocketAddress sender)
    {
        if (sender.equals(config.getLocalAddr()))
        {
            throw new IllegalStateException("this node (a contact node) got a join request from itself, or another claiming the same IP:port");
        }

        // now send the forward join to everyone in my active view
        String id = UUID.randomUUID().toString();
        for (InetSocketAddress peer : Utils.filter(activeView, sender))
        {
            logger.info("sending new FJ to {} about originator {} from seed {}, {}", peer, sender, config.getLocalAddr(), id);
            ForwardJoin forwardJoin = new ForwardJoin(sender, config.getActiveRandomWalkLength(),
                                              config.getActiveRandomWalkLength(), config.getPassiveRandomWalkLength());
            dispatcher.send(this, forwardJoin, peer);
        }

        // add join'ing node to this seed
        addToActiveView(sender);
        dispatcher.send(this, new JoinAck(), sender);
    }

    void addToActiveView(InetSocketAddress peer)
    {
        if (peer.equals(config.getLocalAddr()))
        {
            logger.warn("attempting to add a node to it's own active list. ignoring");
            return;
        }

        if (!activeView.contains(peer))
        {
            activeView.add(peer);
            for (GossipBroadcaster broadcaster : broadcasters)
                broadcaster.neighborUp(peer);


            while (activeView.size() > config.getActiveViewLength())
                removeFromActiveView(activeView.get(0), true);
        }

        // we should never get into the situation where a node is in both the active and passive lists
        passiveView.remove(peer);
    }

    void removeFromActiveView(InetSocketAddress peer, boolean addToPassiveView)
    {
        if (!activeView.remove(peer))
        {
            logger.info("attempted to remove peer {} from the active view, but it was not present.", peer);
            return;
        }

        if (addToPassiveView)
            addToPassiveView(peer);

        dispatcher.send(this, new Disconnect(), peer);

        for (GossipBroadcaster broadcaster : broadcasters)
            broadcaster.neighborDown(peer);
    }

    void addToPassiveView(InetSocketAddress peer)
    {
        if (peer.equals(config.getLocalAddr()) || passiveView.contains(peer))
            return;

        passiveView.add(peer);
        while (passiveView.size() > config.getPassiveViewLength())
            passiveView.remove(0);
    }

    public void handleForwardJoin(final ForwardJoin msg, InetSocketAddress sender)
    {
        int ttl = msg.timeToLive > 0 ? msg.timeToLive - 1 : 0;

        if (ttl == 0 && !activeView.contains(msg.originator))
        {
            logger.info("FORWARD_JOIN - {} adding {} to active view", config.getLocalAddr(), msg.originator);
            addToActiveView(msg.originator);
            dispatcher.send(this, new JoinAck(), msg.originator);
            return;
        }

        // TODO : need to handle case where originator is already in the passive view, perhaps
        // and maybe not decrement the ttl (?) before sending on
        if (ttl == msg.passiveRandomWalkLength)
            addToPassiveView(msg.originator);

        InetSocketAddress peer = Utils.selectRandom(activeView, sender, msg.originator);
        if (peer == null)
            peer = sender;

        logger.debug("sending FORWARD_JOIN to {} for originator {}, ttl = {}", peer, msg.originator, ttl);
        dispatcher.send(this, msg.cloneForForwarding(), peer);
    }


    public void handleJoinACk(JoinAck msg, InetSocketAddress sender)
    {
        addToActiveView(sender);
    }

    public void handleDisconnect(Disconnect msg, InetSocketAddress sender)
    {
        logger.info("{} received disconnect from {}", config.getLocalAddr(), sender);
        if (activeView.contains(sender))
        {
            removeFromActiveView(sender, true);
            sendNeighborRequest(null);
        }
    }

    private void sendNeighborRequest(final InetSocketAddress lastAddr)
    {
        if (passiveView.isEmpty())
        {
            if (activeView.isEmpty())
            {
                logger.info("NR -- as we have no peers, we must rejoin");
                join();
                return;
            }

            // TODO: maybe force a shuffle here? @jcleitao
            logger.info("NR -- passive view is empty, thus cannot send a neighbor request");
            return;
        }

        // TODO: handle case

        Priority priority = activeView.size() == 0 ? Priority.HIGH : Priority.LOW;

        // attempt to asynchronously cycle through the passive view.
        // this is a bit optimistic in that after we send the NEIGHBOR msg,
        // we expect to get some response (async, of course).
        // TODO: we should keep around some timeout reference so we can try another peer
        // (and possibly remove the one that timed out from the passive view).
        int idx = 0;
        if (lastAddr != null)
        {
            idx = Iterables.indexOf(passiveView, new Predicate<InetSocketAddress>()
            {
                public boolean apply(InetSocketAddress input)
                {
                    return lastAddr.equals(input);
                }
            });

            if (idx == -1)
            {
                // if the peer is no longer in the passive view, assumably because it's gone away while we called it
                logger.info("NR -- no more peers in the passive view to send a neighbor request");
                // TODO: decide if it's best to start at the begining of the passive view again, or just bail
                idx = 0;
            }
            else if (idx == passiveView.size() - 1)
            {
                if (activeView.size() == 0)
                {
                    logger.info("at the end of iterating the passive view, but the active view is empty, so try again");
                    idx = 0;
                }
                else
                {
                    logger.info("at the end of iterating the passive view, so stop trying");
                    return;
                }
            }
            else
            {
                idx += 1;
            }
        }


        InetSocketAddress peer = passiveView.get(idx);
        logger.info("NR {} -- idx {}, peer {}, priority {}", config.getLocalAddr(), idx, peer, priority);
        dispatcher.send(this, new NeighborRequest(priority), peer);
    }

    public void handleNeighborRequest(NeighborRequest msg, InetSocketAddress sender)
    {
        if (msg.getPriority() == Priority.LOW && activeView.size() == config.getActiveViewLength())
        {
            dispatcher.send(this, new NeighborResponse(Result.REJECT), sender);
            return;
        }

        addToActiveView(sender);
        dispatcher.send(this, new NeighborResponse(Result.ACCEPT), sender);
    }

    public void handleNeighborResponse(NeighborResponse msg, InetSocketAddress sender)
    {
        logger.info("NRes {} -- peer {}, result {}", config.getLocalAddr(), sender, msg.getResult());
        if (msg.getResult() == Result.ACCEPT)
        {
            addToActiveView(sender);
        }
        else
        {
            if (activeView.size() < config.getActiveViewLength())
            {
                logger.info("NRes {} -- neighbor request was rejected by {}, so try again", config.getLocalAddr(), sender);
                sendNeighborRequest(sender);
            }
        }
    }

    public void handleNextShuffleRound()
    {
        if (activeView.size() == 0)
        {
            // TODO: determine if we really want to force a join here as the process may have just launched
            // and waiting for JOIN to complete. otherwise, yes, I do think we want this
            if (passiveView.size() == 0)
                join();
            else
                sendNeighborRequest(null);
            return;
        }

        InetSocketAddress peer = Utils.selectRandom(activeView);
        if (peer == null)
        {
            logger.warn("pre-condition check of at least one peer in the active view failed. skipping shuffle round");
            return;
        }

        Collection<InetSocketAddress> shufflePeers = buildShuffleGroup(activeView, passiveView);
        Shuffle msg = new Shuffle(config.getLocalAddr(), shufflePeers, config.getShuffleWalkLength());
        dispatcher.send(this, msg, peer);

        //additionally, probabalistically send a shuffle message to a seed node
        final int percentage = 10;
        int rnd = ThreadLocalRandom.current().nextInt(0, percentage);
        if (rnd % percentage == 0)
        {
            InetSocketAddress seed = Utils.selectRandom(config.getSeeds(), peer, config.getLocalAddr());
            if (seed == null)
                return;
            dispatcher.send(this, msg, seed);
        }
    }

    @VisibleForTesting
    Collection<InetSocketAddress> buildShuffleGroup(Collection<InetSocketAddress> activeViewFiltered, Collection<InetSocketAddress> passiveViewFiltered)
    {
        Collection<InetSocketAddress> nodes = new HashSet<>();
        nodes.add(config.getLocalAddr());
        Utils.selectMultipleRandom(activeViewFiltered, nodes, config.getShuffleActiveViewCount());
        Utils.selectMultipleRandom(passiveViewFiltered, nodes, config.getShufflePassiveViewCount());

        return nodes;
    }

    public void handleShuffle(Shuffle msg, InetSocketAddress sender)
    {
        // consume this shuffle message in case this node's active view is somehow zero, else forward along
        if (msg.getTimeToLive() > 0 && activeView.size() > 1)
        {
            logger.debug("in handle_shuffle, going to forward the request from {} on behalf of {}", sender, msg.getOriginator());
            // avoid forwarding the shuffle back to the sender, but if there's nowhere else to send it, return to the sender
            InetSocketAddress peer = Utils.selectRandom(activeView, sender, msg.getOriginator());
            if (peer == null)
                peer = sender;

            dispatcher.send(this, msg.cloneForForwarding(), peer);
            return;
        }

        // build up our response shuffle list before applying the originator's shuffle list
        Collection<InetSocketAddress> shufflePeers = buildShuffleGroup(Utils.filter(activeView, msg.getOriginator()), Utils.filter(passiveView, msg.getOriginator()));
        applyShuffle(msg.getNodes(), Collections.<InetSocketAddress>emptyList());
        dispatcher.send(this, new ShuffleResponse(shufflePeers, msg.getNodes()), msg.getOriginator());
    }

    void applyShuffle(Collection<InetSocketAddress> nodes, Collection<InetSocketAddress> filter)
    {
        // first, filter out any entries that are currently in the active or passive views
        Collection<InetSocketAddress> filtered = Utils.filter(nodes, activeView.toArray(new InetSocketAddress[0]));
        filtered = Utils.filter(filtered, passiveView.toArray(new InetSocketAddress[0]));
        filtered.remove(config.getLocalAddr());

        // next, add any remaining nodes to the passive view
        for (InetSocketAddress peer : filtered)
        {
            if (filter.contains(peer))
                continue;
            addToPassiveView(peer);
        }
    }

    public void handleShuffleReply(ShuffleResponse msg, InetSocketAddress sender)
    {
        applyShuffle(msg.getNodes(), msg.getSentNodes());
    }

    public void handlePeerFailure(InetSocketAddress peer)
    {
        removeFromActiveView(peer, false);
        sendNeighborRequest(null);
    }

    @VisibleForTesting
    // never use outside of testing!!!
    public void addToActiveView(List<InetSocketAddress> nodes)
    {
        for (InetSocketAddress addr : nodes)
        {
            if (!activeView.contains(addr))
                activeView.add(addr);
        }
    }

    @VisibleForTesting
    // never use outside of testing!!!
    public void addToPassiveView(List<InetSocketAddress> nodes)
    {
        for (InetSocketAddress addr : nodes)
        {
            if (!passiveView.contains(addr))
                passiveView.add(addr);
        }
    }

    private class ShuffleRoundStarter implements Runnable
    {
        public void run()
        {
            handleNextShuffleRound();
        }
    }

    @VisibleForTesting
    public HPVConfig getConfig()
    {
        return config;
    }

    @VisibleForTesting
    List<InetSocketAddress> getActiveView()
    {
        return ImmutableList.copyOf(activeView);
    }

    @VisibleForTesting
    List<InetSocketAddress> getPassiveView()
    {
        return ImmutableList.copyOf(passiveView);
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder(256);
        sb.append("\nHyParView service: local addr: ").append(config.getLocalAddr());
        sb.append("\n\tactive view: ").append(activeView);
        sb.append("\n\tpassive view").append(passiveView);
        sb.append("\n");
        return sb.toString();
    }

    public void register(GossipBroadcaster broadcaster)
    {
        if (broadcasters.contains(broadcaster))
            throw new IllegalStateException(String.format("already have gossip broadcaster {} as a registered instance", broadcaster));

        broadcasters.add(broadcaster);
        broadcaster.registered(this);
    }

    public Collection<InetSocketAddress> getPeers()
    {
        return ImmutableList.copyOf(activeView);
    }
}
