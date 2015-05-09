package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
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
import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.Utils;
import org.apache.cassandra.gms2.gossip.peersampling.messages.*;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborRequest.Priority;
import org.apache.cassandra.gms2.gossip.peersampling.messages.NeighborResponse.Result;
import org.apache.cassandra.gms2.membership.PeerSubscriber;

/**
 * An implementation of the HyParView peer sampling service (http://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf).
 *
 * Note: this class is intentionally not thread safe, and assumes a single threaded execution model.
 * This is to keep the implementation simple, and, really, this data/functionality should not be a hot spot.
 */
public class HyParViewService<M extends HyParViewMessage> implements PeerSamplingService, GossipDispatcher.GossipReceiver<M>
{
    private static final Logger logger = LoggerFactory.getLogger(HyParViewService.class);
    private final List<InetAddress> activeView;
    private final List<InetAddress> passiveView;
    private final HPVConfig config;
    private final GossipDispatcher dispatcher;
    private final PeerSubscriber peerSubscriber;

    private final Set<PeerSamplingServiceClient> clients;

    public HyParViewService(HPVConfig config, GossipDispatcher dispatcher, PeerSubscriber peerSubscriber)
    {
        this.config = config;
        this.dispatcher = dispatcher;
        this.peerSubscriber = peerSubscriber;
        activeView = new CopyOnWriteArrayList<>();
        passiveView = new CopyOnWriteArrayList<>();
        clients = new CopyOnWriteArraySet<>();
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
            service.handlePeerFailure(ep);
        }
    }

    public void join()
    {
        InetAddress seed = Utils.selectRandom(config.getSeeds());

        if (seed == null)
        {
            logger.info("no unique seed addresses available to send join request to");
            return;
        }

        // TODO: add callback to ensure we got a response (Neighbor) msg from some peer
        dispatcher.send(this, new Join(), seed);
    }

    public void handle(M msg, InetAddress sender)
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

    public void handleJoin(Join msg, InetAddress sender)
    {
        if (sender.equals(config.getLocalAddr()))
        {
            throw new IllegalStateException("this node (a contact node) got a join request from itself, or another claiming the same IP:port");
        }

        // now send the forward join to everyone in my active view
        String id = UUID.randomUUID().toString();
        for (InetAddress peer : Utils.filter(activeView, sender))
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

    void addToActiveView(InetAddress peer)
    {
        if (peer.equals(config.getLocalAddr()))
        {
            logger.warn("attempting to add a node to it's own active list. ignoring");
            return;
        }

        if (!activeView.contains(peer))
        {
            activeView.add(peer);
            for (PeerSamplingServiceClient client : clients)
                client.neighborUp(peer);

            while (activeView.size() > config.getActiveViewLength())
                removeFromActiveView(activeView.get(0), true);
        }

        // we should never get into the situation where a node is in both the active and passive lists
        passiveView.remove(peer);
    }

    void removeFromActiveView(InetAddress peer, boolean addToPassiveView)
    {
        if (!activeView.remove(peer))
        {
            logger.info("attempted to remove peer {} from the active view, but it was not present.", peer);
            return;
        }

        if (addToPassiveView)
            addToPassiveView(peer);

        dispatcher.send(this, new Disconnect(), peer);

        for (PeerSamplingServiceClient client : clients)
            client.neighborDown(peer);
    }

    void addToPassiveView(InetAddress peer)
    {
        if (peer.equals(config.getLocalAddr()) || passiveView.contains(peer))
            return;

        passiveView.add(peer);
        while (passiveView.size() > config.getPassiveViewLength())
            passiveView.remove(0);
    }

    public void handleForwardJoin(final ForwardJoin msg, InetAddress sender)
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

        InetAddress peer = Utils.selectRandom(activeView, sender, msg.originator);
        if (peer == null)
            peer = sender;

        logger.debug("sending FORWARD_JOIN to {} for originator {}, ttl = {}", peer, msg.originator, ttl);
        dispatcher.send(this, msg.cloneForForwarding(), peer);
    }


    public void handleJoinACk(JoinAck msg, InetAddress sender)
    {
        addToActiveView(sender);
    }

    public void handleDisconnect(Disconnect msg, InetAddress sender)
    {
        logger.info("{} received disconnect from {}", config.getLocalAddr(), sender);
        if (activeView.contains(sender))
        {
            removeFromActiveView(sender, true);
            sendNeighborRequest(null);
        }
    }

    private void sendNeighborRequest(final InetAddress lastAddr)
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
            idx = Iterables.indexOf(passiveView, new Predicate<InetAddress>()
            {
                public boolean apply(InetAddress input)
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


        InetAddress peer = passiveView.get(idx);
        logger.info("NR {} -- idx {}, peer {}, priority {}", config.getLocalAddr(), idx, peer, priority);
        dispatcher.send(this, new NeighborRequest(priority), peer);
    }

    public void handleNeighborRequest(NeighborRequest msg, InetAddress sender)
    {
        if (msg.getPriority() == Priority.LOW && activeView.size() == config.getActiveViewLength())
        {
            dispatcher.send(this, new NeighborResponse(Result.REJECT), sender);
            return;
        }

        addToActiveView(sender);
        dispatcher.send(this, new NeighborResponse(Result.ACCEPT), sender);
    }

    public void handleNeighborResponse(NeighborResponse msg, InetAddress sender)
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

        InetAddress peer = Utils.selectRandom(activeView);
        if (peer == null)
        {
            logger.warn("pre-condition check of at least one peer in the active view failed. skipping shuffle round");
            return;
        }

        Collection<InetAddress> shufflePeers = buildShuffleGroup(activeView, passiveView);
        Shuffle msg = new Shuffle(config.getLocalAddr(), shufflePeers, config.getShuffleWalkLength());
        dispatcher.send(this, msg, peer);

        //additionally, probabalistically send a shuffle message to a seed node
        final int percentage = 10;
        int rnd = ThreadLocalRandom.current().nextInt(0, percentage);
        if (rnd % percentage == 0)
        {
            InetAddress seed = Utils.selectRandom(config.getSeeds(), peer, config.getLocalAddr());
            if (seed == null)
                return;
            dispatcher.send(this, msg, seed);
        }
    }

    @VisibleForTesting
    Collection<InetAddress> buildShuffleGroup(Collection<InetAddress> activeViewFiltered, Collection<InetAddress> passiveViewFiltered)
    {
        Collection<InetAddress> nodes = new HashSet<>();
        nodes.add(config.getLocalAddr());
        Utils.selectMultipleRandom(activeViewFiltered, nodes, config.getShuffleActiveViewCount());
        Utils.selectMultipleRandom(passiveViewFiltered, nodes, config.getShufflePassiveViewCount());

        return nodes;
    }

    public void handleShuffle(Shuffle msg, InetAddress sender)
    {
        // consume this shuffle message in case this node's active view is somehow zero, else forward along
        if (msg.getTimeToLive() > 0 && activeView.size() > 1)
        {
            logger.debug("in handle_shuffle, going to forward the request from {} on behalf of {}", sender, msg.getOriginator());
            // avoid forwarding the shuffle back to the sender, but if there's nowhere else to send it, return to the sender
            InetAddress peer = Utils.selectRandom(activeView, sender, msg.getOriginator());
            if (peer == null)
                peer = sender;

            dispatcher.send(this, msg.cloneForForwarding(), peer);
            return;
        }

        // build up our response shuffle list before applying the originator's shuffle list
        Collection<InetAddress> shufflePeers = buildShuffleGroup(Utils.filter(activeView, msg.getOriginator()), Utils.filter(passiveView, msg.getOriginator()));
        applyShuffle(msg.getNodes(), Collections.<InetAddress>emptyList());
        dispatcher.send(this, new ShuffleResponse(shufflePeers, msg.getNodes()), msg.getOriginator());
    }

    void applyShuffle(Collection<InetAddress> nodes, Collection<InetAddress> filter)
    {
        // first, filter out any entries that are currently in the active or passive views
        Collection<InetAddress> filtered = Utils.filter(nodes, activeView.toArray(new InetAddress[0]));
        filtered = Utils.filter(filtered, passiveView.toArray(new InetAddress[0]));
        filtered.remove(config.getLocalAddr());

        // next, add any remaining nodes to the passive view
        for (InetAddress peer : filtered)
        {
            if (filter.contains(peer))
                continue;
            addToPassiveView(peer);
        }
    }

    public void handleShuffleReply(ShuffleResponse msg, InetAddress sender)
    {
        applyShuffle(msg.getNodes(), msg.getSentNodes());
    }

    public void handlePeerFailure(InetAddress peer)
    {
        removeFromActiveView(peer, false);
        sendNeighborRequest(null);
    }

    @VisibleForTesting
    // never use outside of testing!!!
    public void addToActiveView(List<InetAddress> nodes)
    {
        for (InetAddress addr : nodes)
        {
            if (!activeView.contains(addr))
                activeView.add(addr);
        }
    }

    @VisibleForTesting
    // never use outside of testing!!!
    public void addToPassiveView(List<InetAddress> nodes)
    {
        for (InetAddress addr : nodes)
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

    public InetAddress getAddress()
    {
        return config.getLocalAddr();
    }

    @VisibleForTesting
    public HPVConfig getConfig()
    {
        return config;
    }

    @VisibleForTesting
    List<InetAddress> getActiveView()
    {
        return ImmutableList.copyOf(activeView);
    }

    @VisibleForTesting
    List<InetAddress> getPassiveView()
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

    public void register(PeerSamplingServiceClient client)
    {
        if (clients.contains(client))
            throw new IllegalStateException(String.format("already have gossip broadcaster {} as a registered instance", client));

        clients.add(client);
        client.registered(this);
    }

    public Collection<InetAddress> getPeers()
    {
        return ImmutableList.copyOf(activeView);
    }
}
