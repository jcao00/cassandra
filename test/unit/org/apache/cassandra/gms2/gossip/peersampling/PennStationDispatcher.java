package org.apache.cassandra.gms2.gossip.peersampling;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.gms2.gossip.GossipDispatcher;
import org.apache.cassandra.gms2.gossip.peersampling.messages.HyParViewMessage;

/**
 * ... because GrandCentralDispatch was already in use ... but, hey,
 * it's still better than the Port Authority Bus Terminal!
 */
public class PennStationDispatcher implements GossipDispatcher
{
    private final ConcurrentHashMap<InetAddress, HyParViewService> nodes;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger cnt = new AtomicInteger(0);

    public PennStationDispatcher()
    {
        nodes = new ConcurrentHashMap<>();
        executor = new ThreadPoolExecutor(2, 32, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    }

    public void register(InetAddress addr, HyParViewService svc)
    {
        nodes.put(addr, svc);
    }

    public ConcurrentHashMap<InetAddress, HyParViewService> getNodes()
    {
        return nodes;
    }

    public void send(HyParViewService svc, HyParViewMessage msg, InetAddress dest)
    {
        HyParViewService destSvc = nodes.get(dest);
        if (destSvc == null)
            throw new IllegalArgumentException("no registered destination service with addr " + dest);

        cnt.incrementAndGet();
        executor.submit(new WorkTask(destSvc, msg, svc.getConfig().getLocalAddr()));
    }

    private static class WorkTask implements Runnable
    {
        private final HyParViewService destination;
        private final HyParViewMessage msg;
        private final InetAddress sender;

        private WorkTask(HyParViewService destination, HyParViewMessage msg, InetAddress sender)
        {
            this.destination = destination;
            this.msg = msg;
            this.sender = sender;
        }

        public void run()
        {
            destination.handle(msg, sender);
        }
    }

    // a loose metric to see if there's any messages left in the queue or work being processed
    public boolean stillWorking()
    {
        return cnt.get() > executor.getCompletedTaskCount();
//        return 0 != executor.getActiveCount() + executor.getQueue().size();// + executor.getCompletedTaskCount();
    }
}
