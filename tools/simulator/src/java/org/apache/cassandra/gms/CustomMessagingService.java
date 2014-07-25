package org.apache.cassandra.gms;

import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomMessagingService implements GossipDigestMessageSender
{
    public static final Map<String, byte[]> parameters = new HashMap<>();

    public final Map<InetAddress, Gossiper> gossipers = new ConcurrentHashMap<>();
    private final Random random;
    private final AtomicInteger idGen = new AtomicInteger(0);

    public CustomMessagingService()
    {
        random = new Random(System.nanoTime());
    }

    public void sendOneWay(MessageOut message, InetAddress to, Gossiper sender)
    {
        Gossiper target = gossipers.get(to);
        if (target == null)
            throw new IllegalArgumentException("unknown peer addr: " + to);
        generateDelay(sender, target);

        switch (message.verb)
        {
            case GOSSIP_DIGEST_SYN:
                MessageIn<GossipDigestSyn> synMsg = MessageIn.create(message.from, (GossipDigestSyn)message.payload, parameters, message.verb, 0);
                new GossipDigestSynVerbHandler(sender, this).doVerb(synMsg, idGen.incrementAndGet());
                break;
            case GOSSIP_DIGEST_ACK:
                MessageIn<GossipDigestAck> ackMsg = MessageIn.create(message.from, (GossipDigestAck)message.payload, parameters, message.verb, 0);
                new GossipDigestAckVerbHandler(sender, this).doVerb(ackMsg, idGen.incrementAndGet());
                break;
            case GOSSIP_DIGEST_ACK2:
                MessageIn<GossipDigestAck2> msg = MessageIn.create(message.from, (GossipDigestAck2)message.payload, parameters, message.verb, 0);
                new GossipDigestAck2VerbHandler(sender).doVerb(msg, idGen.incrementAndGet());
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

        try
        {
            Thread.sleep(delay);
        }
        catch (InterruptedException e)
        {
            //nop
        }
//        Uninterruptibles.sleepUninterruptibly(delay, TimeUnit.MICROSECONDS);
    }


    public void register(InetAddress addr, Gossiper gossiper)
    {
        gossipers.put(addr, gossiper);
    }
}
