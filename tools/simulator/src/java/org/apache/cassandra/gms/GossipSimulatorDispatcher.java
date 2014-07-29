package org.apache.cassandra.gms;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GossipSimulatorDispatcher implements GossipDigestMessageSender
{
    private static final Logger logger = LoggerFactory.getLogger(GossipSimulatorDispatcher.class);
    public static final Map<String, byte[]> parameters = new HashMap<>();

    public final Map<InetAddress, Gossiper> gossipers = new ConcurrentHashMap<>();
    private final Random random;
    private final AtomicInteger idGen = new AtomicInteger(0);
    private CyclicBarrier barrier;

    public GossipSimulatorDispatcher(CyclicBarrier barrier)
    {
        this.barrier = barrier;
        random = new Random(System.nanoTime());
    }

    public void setBarrier(CyclicBarrier barrier)
    {
        this.barrier = barrier;
    }

    public void sendOneWay(MessageOut message, InetAddress to, Gossiper sender)
    {
        Gossiper target = gossipers.get(to);
        if (target == null)
            throw new IllegalArgumentException("unknown peer addr: " + to);
        generateDelay(sender, target);
        int id = idGen.incrementAndGet();
        logger.trace("sending {} from {} to {}", message.verb, message.from, to);

        switch (message.verb)
        {
            case GOSSIP_DIGEST_SYN:
                MessageIn<GossipDigestSyn> synMsg = MessageIn.create(message.from, (GossipDigestSyn)message.payload, parameters, message.verb, id);
                new GossipDigestSynVerbHandler(target, this).doVerb(synMsg, idGen.incrementAndGet());
                break;
            case GOSSIP_DIGEST_ACK:
                MessageIn<GossipDigestAck> ackMsg = MessageIn.create(message.from, (GossipDigestAck)message.payload, parameters, message.verb, id);
                new GossipDigestAckVerbHandler(target, this).doVerb(ackMsg, idGen.incrementAndGet());
                break;
            case GOSSIP_DIGEST_ACK2:
                MessageIn<GossipDigestAck2> msg = MessageIn.create(message.from, (GossipDigestAck2)message.payload, parameters, message.verb, id);
                new GossipDigestAck2VerbHandler(target).doVerb(msg, idGen.incrementAndGet());
                break;
        }
    }

    private void generateDelay(Gossiper sender, Gossiper target)
    {
        // would love to do some neato probability distributions, but, alas, I'm not smart enough <sigh>
        double d = random.nextDouble();
        long delay = (long)(10000L * d);


        // TODO: if nodes in different DCs, add a few millis
        // TODO: if nodes in different racks, add a few hundreds of micros

        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MICROSECONDS);
    }

    public void sendRR(MessageOut message, InetAddress to, IAsyncCallback callback, Gossiper sender)
    {
        MessageIn<EchoMessage> response = MessageIn.create(to, (EchoMessage)message.payload, parameters, message.verb, idGen.incrementAndGet());
        callback.response(response);
    }

    public boolean blockUntilReady()
    {
        try
        {
            barrier.await();
        }
        catch (BrokenBarrierException | InterruptedException e)
        {
            return false;
        }
        return true;
    }

    public void register(InetAddress addr, Gossiper gossiper)
    {
        gossipers.put(addr, gossiper);
    }
}
