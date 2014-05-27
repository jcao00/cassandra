/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.*;
import org.apache.cassandra.repair.messages.AnticompactionRequest;
import org.apache.cassandra.repair.messages.CancelRepairRequest;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ActiveRepairService is the starting point for manual "active" repairs.
 *
 * Each user triggered repair will correspond to one or multiple repair session,
 * one for each token range to repair. On repair session might repair multiple
 * column families. For each of those column families, the repair session will
 * request merkle trees for each replica of the range being repaired, diff those
 * trees upon receiving them, schedule the streaming ofthe parts to repair (based on
 * the tree diffs) and wait for all those operation. See RepairSession for more
 * details.
 *
 * The creation of a repair session is done through the submitRepairSession that
 * returns a future on the completion of that session.
 */
public class ActiveRepairService
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveRepairService.class);
    // singleton enforcement
    public static final ActiveRepairService instance = new ActiveRepairService();

    public static final long UNREPAIRED_SSTABLE = 0;

    private static final ThreadPoolExecutor executor;
    static
    {
        executor = new JMXConfigurableThreadPoolExecutor(4,
                                                         60,
                                                         TimeUnit.SECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         new NamedThreadFactory("AntiEntropySessions"),
                                                         "internal");
    }

    public static enum Status
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    /**
     * A map of active coordinator session.
     */
    private final ConcurrentMap<UUID, RepairSession> sessions;

    private final ConcurrentMap<UUID, ParentRepairSession> parentRepairSessions;

    /**
     * Protected constructor. Use ActiveRepairService.instance.
     */
    protected ActiveRepairService()
    {
        sessions = new ConcurrentHashMap<>();
        parentRepairSessions = new ConcurrentHashMap<>();
    }

    /**
     * Requests repairs for the given keyspace and column families.
     *
     * @return Future for asynchronous call or null if there is no need to repair
     */
    public RepairFuture submitRepairSession(UUID parentRepairSession, Range<Token> range, String keyspace, boolean isSequential, Set<InetAddress> endpoints, String... cfnames)
    {
        RepairSession session = new RepairSession(parentRepairSession, range, keyspace, isSequential, endpoints, cfnames);
        if (session.endpoints.isEmpty())
            return null;
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    public void addToActiveSessions(RepairSession session)
    {
        sessions.put(session.getId(), session);
        Gossiper.instance.register(session);
        FailureDetector.instance.registerFailureDetectionEventListener(session);
    }

    public void removeFromActiveSessions(RepairSession session)
    {
        Gossiper.instance.unregister(session);
        sessions.remove(session.getId());
    }

    public void terminateSessions()
    {
        for (RepairSession session : sessions.values())
        {
            session.forceShutdown();
        }
        parentRepairSessions.clear();
    }

    // for testing only. Create a session corresponding to a fake request and
    // add it to the sessions (avoid NPE in tests)
    RepairFuture submitArtificialRepairSession(RepairJobDesc desc)
    {
        Set<InetAddress> neighbours = new HashSet<>();
        neighbours.addAll(ActiveRepairService.getNeighbors(desc.keyspace, desc.range, null, null));
        RepairSession session = new RepairSession(desc.parentSessionId, desc.sessionId, desc.range, desc.keyspace, false, neighbours, new String[]{desc.columnFamily});
        sessions.put(session.getId(), session);
        RepairFuture futureTask = new RepairFuture(session);
        executor.execute(futureTask);
        return futureTask;
    }

    /**
     * Return all of the neighbors with whom we share the provided range.
     *
     * @param keyspaceName keyspace to repair
     * @param toRepair token to repair
     * @param dataCenters the data centers to involve in the repair
     *
     * @return neighbors with whom we share the provided range
     */
    public static Set<InetAddress> getNeighbors(String keyspaceName, Range<Token> toRepair, Collection<String> dataCenters, Collection<String> hosts)
    {
        StorageService ss = StorageService.instance;
        Map<Range<Token>, List<InetAddress>> replicaSets = ss.getRangeToAddressMap(keyspaceName);
        Range<Token> rangeSuperSet = null;
        for (Range<Token> range : ss.getLocalRanges(keyspaceName))
        {
            if (range.contains(toRepair))
            {
                rangeSuperSet = range;
                break;
            }
            else if (range.intersects(toRepair))
            {
                throw new IllegalArgumentException("Requested range intersects a local range but is not fully contained in one; this would lead to imprecise repair");
            }
        }
        if (rangeSuperSet == null || !replicaSets.containsKey(rangeSuperSet))
            return Collections.emptySet();

        Set<InetAddress> neighbors = new HashSet<>(replicaSets.get(rangeSuperSet));
        neighbors.remove(FBUtilities.getBroadcastAddress());

        if (dataCenters != null)
        {
            TokenMetadata.Topology topology = ss.getTokenMetadata().cloneOnlyTokenMap().getTopology();
            Set<InetAddress> dcEndpoints = Sets.newHashSet();
            Multimap<String,InetAddress> dcEndpointsMap = topology.getDatacenterEndpoints();
            for (String dc : dataCenters)
            {
                Collection<InetAddress> c = dcEndpointsMap.get(dc);
                if (c != null)
                   dcEndpoints.addAll(c);
            }
            return Sets.intersection(neighbors, dcEndpoints);
        }
        else if (hosts != null)
        {
            Set<InetAddress> specifiedHost = new HashSet<>();
            for (final String host : hosts)
            {
                try
                {
                    final InetAddress endpoint = InetAddress.getByName(host.trim());
                    if (endpoint.equals(FBUtilities.getBroadcastAddress()) || neighbors.contains(endpoint))
                        specifiedHost.add(endpoint);
                }
                catch (UnknownHostException e)
                {
                    throw new IllegalArgumentException("Unknown host specified " + host, e);
                }
            }

            if (!specifiedHost.contains(FBUtilities.getBroadcastAddress()))
                throw new IllegalArgumentException("The current host must be part of the repair");

            if (specifiedHost.size() <= 1)
            {
                String msg = "Repair requires at least two endpoints that are neighbours before it can continue, the endpoint used for this repair is %s, " +
                             "other available neighbours are %s but these neighbours were not part of the supplied list of hosts to use during the repair (%s).";
                throw new IllegalArgumentException(String.format(msg, specifiedHost, neighbors, hosts));
            }

            specifiedHost.remove(FBUtilities.getBroadcastAddress());
            return specifiedHost;

        }

        return neighbors;
    }

    public UUID prepareForRepair(Set<InetAddress> endpoints, Collection<Range<Token>> ranges, List<ColumnFamilyStore> columnFamilyStores, boolean incrementalRepair)
    {
        UUID parentRepairSession = UUIDGen.getTimeUUID();
        registerParentRepairSession(parentRepairSession, columnFamilyStores, ranges, incrementalRepair);

        List<UUID> cfIds = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfIds.add(cfs.metadata.cfId);

        LatchCallback callback = new LatchCallback(endpoints.size());
        for(InetAddress neighbour : endpoints)
        {
            PrepareMessage message = new PrepareMessage(parentRepairSession, cfIds, ranges, incrementalRepair);
            MessageOut<RepairMessage> msg = message.createMessage();
            MessagingService.instance().sendRRWithFailure(msg, neighbour, callback);
        }

        try
        {
            callback.await(1, TimeUnit.HOURS);
        }
        catch (RuntimeException re)
        {
            parentRepairSessions.remove(parentRepairSession);
            //TODO: perhaps to send out a cancel message (as a last ditch effort for cleanup)?
            throw re;
        }

        return parentRepairSession;
    }

    static class LatchCallback implements IAsyncCallbackWithFailure
    {
        private final CountDownLatch latch;
        private final AtomicBoolean status;

        LatchCallback(int cnt)
        {
            this.latch = new CountDownLatch(cnt);
            this.status = new AtomicBoolean(true);
        }

        public void response(MessageIn msg)
        {
            latch.countDown();
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public void onFailure(InetAddress from)
        {
            status.set(false);
            latch.countDown();
        }

        public void await(long timeout, TimeUnit unit) throws RuntimeException
        {
            try
            {
                latch.await(timeout, unit);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException("Did not get replies from all endpoints.", e);
            }

            if (!status.get())
                throw new RuntimeException("Did not get positive replies from all endpoints.");
        }
    }

    public void registerParentRepairSession(UUID parentRepairSession, List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, boolean incrementalRepair)
    {
        final long repairedAt = System.currentTimeMillis();
        if (incrementalRepair)
        {
            Map<UUID, Set<SSTableReader>> sstablesToRepair = new HashMap<>();
            for (ColumnFamilyStore cfs : columnFamilyStores)
            {
                Set<SSTableReader> sstables = new HashSet<>();
                for (SSTableReader sstable : cfs.getSSTables())
                {
                    if (new Bounds<>(sstable.first.getToken(), sstable.last.getToken()).intersects(ranges))
                    {
                        if (!sstable.isRepaired())
                        {
                            sstables.add(sstable);
                        }
                    }
                }
                sstablesToRepair.put(cfs.metadata.cfId, sstables);
            }
            parentRepairSessions.put(parentRepairSession, new IncrementalRepairSession(columnFamilyStores, ranges, sstablesToRepair, repairedAt));
        }
        else
        {
            parentRepairSessions.put(parentRepairSession, new FullRepairSession(columnFamilyStores, ranges, repairedAt));
        }
    }

    public void finishParentSession(UUID parentSession, Set<InetAddress> neighbors) throws InterruptedException, ExecutionException, IOException
    {
        ParentRepairSession prs = parentRepairSessions.get(parentSession);
        if (!prs.isIncremetalRepair())
        {
            parentRepairSessions.remove(parentSession);
            return;
        }
        parentRepairSessions.put(parentRepairSession, new ParentRepairSession(columnFamilyStores, ranges, sstablesToRepair, System.currentTimeMillis()));
    }

    public void finishParentSession(UUID parentSession, Set<InetAddress> neighbors, boolean doAntiCompaction) throws InterruptedException, ExecutionException, IOException
    {
        for (InetAddress neighbor : neighbors)
        {
            AnticompactionRequest acr = new AnticompactionRequest(parentSession);
            MessageOut<RepairMessage> req = acr.createMessage();
            MessagingService.instance().sendOneWay(req, neighbor);
        }

        try
        {
            if (doAntiCompaction)
            {
                for (InetAddress neighbor : neighbors)
                {
                    AnticompactionRequest acr = new AnticompactionRequest(parentSession);
                    MessageOut<RepairMessage> req = acr.createMessage();
                    MessagingService.instance().sendOneWay(req, neighbor);
                }
                List<Future<?>> futures = doAntiCompaction(parentSession);
                FBUtilities.waitOnFutures(futures);
            }
        }
        finally
        {
            parentRepairSessions.remove(parentSession);
        }
    }

    public ParentRepairSession getParentRepairSession(UUID parentSessionId)
    {
        return parentRepairSessions.get(parentSessionId);
    }

    public List<Future<?>> doAntiCompaction(UUID parentRepairSession) throws InterruptedException, ExecutionException, IOException
    {
        assert parentRepairSession != null;
        List<Future<?>> futures = new ArrayList<>();
        ParentRepairSession prs = getParentRepairSession(parentRepairSession);
        if (!prs.isIncremetalRepair())
            return futures;

        for (Map.Entry<UUID, ColumnFamilyStore> columnFamilyStoreEntry : prs.columnFamilyStores.entrySet())
        {
            Collection<SSTableReader> sstables = new HashSet<>(prs.getAndReferenceSSTables(columnFamilyStoreEntry.getValue()));
            ColumnFamilyStore cfs = columnFamilyStoreEntry.getValue();
            boolean success = false;
            while (!success)
            {
                for (SSTableReader compactingSSTable : cfs.getDataTracker().getCompacting())
                {
                    if (sstables.remove(compactingSSTable))
                        SSTableReader.releaseReferences(Arrays.asList(compactingSSTable));
                }
                success = sstables.isEmpty() || cfs.getDataTracker().markCompacting(sstables);
            }

            futures.add(CompactionManager.instance.submitAntiCompaction(cfs, sstables, prs));
        }

        return futures;
    }

    public void handleMessage(InetAddress endpoint, RepairMessage message)
    {
        RepairJobDesc desc = message.desc;
        RepairSession session = sessions.get(desc.sessionId);
        if (session == null)
            return;
        switch (message.messageType)
        {
            case VALIDATION_COMPLETE:
                ValidationComplete validation = (ValidationComplete) message;
                session.validationComplete(desc, endpoint, validation.tree);
                break;
            case SYNC_COMPLETE:
                // one of replica is synced.
                SyncComplete sync = (SyncComplete) message;
                session.syncComplete(desc, sync.nodes, sync.success);
                break;
            default:
                break;
        }
    }

    public void cancelRepair(UUID parentSessionUuid)
    {
        assert parentSessionUuid != null;
        ParentRepairSession prs = parentRepairSessions.get(parentSessionUuid);
        if (prs == null)
            throw new RuntimeException("could not find a repair session with identifier " + parentSessionUuid);

        // first, cancel any subordinate sessions (for the given parent) running on this node
        // will only apply if this node is the initiator/coordinator of the repair session
        List<LatchCallback> callbacks = new LinkedList<>();
        for (RepairSession rs : sessions.values())
        {
            if (rs.parentRepairSession != parentSessionUuid)
                continue;
            rs.forceShutdown();

            //send remote cancellation request
            LatchCallback callback = new LatchCallback(rs.endpoints.size());
            for(InetAddress neighbour : rs.endpoints)
            {
                CancelRepairRequest message = new CancelRepairRequest(parentSessionUuid);
                MessageOut<RepairMessage> msg = message.createMessage();
                MessagingService.instance().sendRRWithFailure(msg, neighbour, callback);
            }
            callbacks.add(callback);
            //cancel local coordinator's running validation/sync
            removeFromActiveSessions(rs);
        }

        // next, terminate any operations this node is doing for the repair (applies to all nodes in repair)
        terminateRepairOperations(parentSessionUuid);

        //sit back and wait for any peers to ack the repair cancel message
        for (LatchCallback callback : callbacks)
        {
            try
            {
                callback.await(5, TimeUnit.MINUTES);
            }
            catch (RuntimeException re)
            {
                //swallow exception - maybe log??
            }
        }

        parentRepairSessions.remove(parentSessionUuid);
    }

    /** stop locally executing validation compactions or sync sessions for the given UUID */
    public boolean terminateRepairOperations(UUID parentRepairSession)
    {
        // make sure this node is currently involved in the given repair session
        ParentRepairSession prs = parentRepairSessions.get(parentRepairSession);
        if (prs == null)
            return false;

        prs.cancel();

        if (Validator.cancel(parentRepairSession))
            return true;

        if (StreamingRepairTask.cancel(parentRepairSession))
            return true;

        return false;
    }

    public abstract static class ParentRepairSession
    {
        public final Map<UUID, ColumnFamilyStore> columnFamilyStores = new HashMap<>();
        public final Collection<Range<Token>> ranges;
        public final long repairedAt;
        private volatile boolean cancelled;

        public ParentRepairSession(List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, long repairedAt)
        {
            for (ColumnFamilyStore cfs : columnFamilyStores)
                this.columnFamilyStores.put(cfs.metadata.cfId, cfs);
            this.ranges = ranges;
            this.repairedAt = repairedAt;
        }

        public abstract Collection<SSTableReader> getAndReferenceSSTables(ColumnFamilyStore cfs);

        public abstract boolean isIncremetalRepair();

        public void cancel()
        {
            cancelled = true;
        }

        public boolean isCancelled()
        {
            return cancelled;
        }
    }

    public static class IncrementalRepairSession extends ParentRepairSession
    {
        public final Map<UUID, Set<SSTableReader>> sstableMap;

        public IncrementalRepairSession(List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, Map<UUID, Set<SSTableReader>> sstables, long repairedAt)
        {
            super(columnFamilyStores, ranges, repairedAt);
            this.sstableMap = sstables;
        }

        public Collection<SSTableReader> getAndReferenceSSTables(ColumnFamilyStore cfs)
        {
            Set<SSTableReader> sstables = sstableMap.get(cfs.metadata.cfId);
            Iterator<SSTableReader> sstableIterator = sstables.iterator();
            while (sstableIterator.hasNext())
            {
                SSTableReader sstable = sstableIterator.next();
                if (!new File(sstable.descriptor.filenameFor(Component.DATA)).exists())
                {
                    sstableIterator.remove();
                }
                else
                {
                    if (!sstable.acquireReference())
                        sstableIterator.remove();
                }
            }
            return sstables;
        }

        public boolean isIncremetalRepair()
        {
            return true;
        }
    }

    public static class FullRepairSession extends ParentRepairSession
    {
        public FullRepairSession(List<ColumnFamilyStore> columnFamilyStores, Collection<Range<Token>> ranges, long repairedAt)
        {
            super(columnFamilyStores, ranges, repairedAt);
        }

        public Collection<SSTableReader> getAndReferenceSSTables(ColumnFamilyStore cfs)
        {
            if (columnFamilyStores.containsKey(cfs.metadata.cfId))
                return cfs.markCurrentSSTablesReferenced();
            return Collections.EMPTY_LIST;
        }

        public boolean isIncremetalRepair()
        {
            return false;
        }
    }
}
