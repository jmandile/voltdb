/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.RecoveryMessage;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltdb.RecoverySiteProcessor.MessageHandler;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.export.processors.RawProcessor;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.iv2.JoinProducerBase;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RejoinMessage.Type;
import org.voltdb.rejoin.StreamSnapshotSink;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.SnapshotResponseHandler;
import org.voltdb.utils.CachedByteBufferAllocator;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;

import com.google.common.collect.ImmutableMap;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite
implements Runnable, SiteProcedureConnection, SiteSnapshotConnection
{
    private VoltLogger m_txnlog;
    private final VoltLogger m_rejoinLog = new VoltLogger("REJOIN");
    private static final VoltLogger log = new VoltLogger("EXEC");
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int siteIndex = siteIndexCounter.getAndIncrement();
    private final ExecutionSiteNodeFailureFaultHandler m_faultHandler =
        new ExecutionSiteNodeFailureFaultHandler();

    final LoadedProcedureSet m_loadedProcedures;
    final Mailbox m_mailbox;
    final ExecutionEngine ee;
    final HsqlBackend hsql;
    public volatile boolean m_shouldContinue = true;

    private PartitionDRGateway m_partitionDRGateway = null;

    /*
     * Recover a site at a time to make the interval in which other sites
     * are blocked as small as possible. The permit will be generated once.
     * The permit is only acquired by recovering partitions and not the source
     * partitions.
     */
    public static final Semaphore m_recoveryPermit = new Semaphore(Integer.MAX_VALUE);

    private boolean m_rejoining = false;
    private boolean m_haveRecoveryPermit = false;
    private long m_recoveryStartTime = 0;
    private static AtomicLong m_recoveryBytesTransferred = new AtomicLong();

    // Catalog
    public CatalogContext m_context;
    protected SiteTracker m_tracker;

    final long m_siteId;
    public long getSiteId() {
        return m_siteId;
    }

    HashMap<Long, TransactionState> m_transactionsById = new HashMap<Long, TransactionState>();

    private TransactionState m_currentTransactionState;

    // The time in ms since epoch of the last call to tick()
    long lastTickTime = 0;
    long lastCommittedTxnId = 0;
    long lastCommittedTxnTime = 0;

    /*
     * Due to failures we may find out about commited multi-part txns
     * before running the commit fragment. Handle node fault will generate
     * the fragment, but it is possible for a new failure to be detected
     * before the fragment can be run due to the order messages are pulled
     * from subjects. Maintain and send this value when discovering/sending
     * failure data.
     *
     * This value only gets updated on multi-partition transactions that are
     * not read-only.
     */
    long lastKnownGloballyCommitedMultiPartTxnId = 0;

    public final static long kInvalidUndoToken = -1L;
    private long latestUndoToken = 0L;

    @Override
    public long getLatestUndoToken() {
        return latestUndoToken;
    }

    @Override
    public long getNextUndoToken() {
        return ++latestUndoToken;
    }

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;

    private RecoverySiteProcessor m_recoveryProcessor = null;
    // The following variables are used for new rejoin
    private StreamSnapshotSink m_rejoinSnapshotProcessor = null;
    private volatile long m_rejoinSnapshotTxnId = -1;
    // The snapshot completion handler will set this to true
    private volatile boolean m_rejoinSnapshotFinished = false;
    private long m_rejoinSnapshotBytes = 0;
    private long m_rejoinCoordinatorHSId = -1;
    private TaskLog m_rejoinTaskLog = null;
    // Used to track if the site can keep up on rejoin, default is 10 seconds
    private static final long MAX_BEHIND_DURATION =
            Long.parseLong(System.getProperty("MAX_REJOIN_BEHIND_DURATION", "10000"));
    private long m_lastTimeMadeProgress = 0;
    private long m_remainingTasks = 0;
    private long m_executedTaskCount = 0;
    private long m_loggedTaskCount = 0;
    private long m_taskExeStartTime = 0;
    private final SnapshotCompletionInterest m_snapshotCompletionHandler =
            new SnapshotCompletionInterest() {
        @Override
        public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event) {
            if (m_rejoinSnapshotTxnId != -1) {
                if (m_rejoinSnapshotTxnId == event.multipartTxnId) {
                    m_rejoinLog.debug("Rejoin snapshot for site " + getSiteId() +
                                        " is finished");
                    VoltDB.instance().getSnapshotCompletionMonitor().removeInterest(this);
                    // Notify the rejoin coordinator so that it can start the next site
                    if (m_rejoinCoordinatorHSId != -1) {
                        RejoinMessage msg =
                                new RejoinMessage(getSiteId(), RejoinMessage.Type.SNAPSHOT_FINISHED);
                        m_mailbox.send(m_rejoinCoordinatorHSId, msg);
                    }
                    m_rejoinSnapshotFinished = true;
                }
            }
            return new CountDownLatch(0);
        }
    };

    // Trigger if shutdown has been run already.
    private boolean haveShutdownAlready;

    // This message is used to start a local snapshot. The snapshot
    // is *not* automatically coordinated across the full node set.
    // That must be arranged separately.
    public static class ExecutionSiteLocalSnapshotMessage extends VoltMessage
    {
        public final String path;
        public final String nonce;
        public final boolean crash;

        /**
         * @param roadblocktxnid
         * @param path
         * @param nonce
         * @param crash Should Volt crash itself afterwards
         */
        public ExecutionSiteLocalSnapshotMessage(long roadblocktxnid,
                                                 String path,
                                                 String nonce,
                                                 boolean crash) {
            m_roadblockTransactionId = roadblocktxnid;
            this.path = path;
            this.nonce = nonce;
            this.crash = crash;
        }

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }

        long m_roadblockTransactionId;

        @Override
        protected void initFromBuffer(ByteBuffer buf)
        {
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf)
        {
        }
    }

    // This message is used locally to schedule a node failure event's
    // required  processing at an execution site.
    class ExecutionSiteNodeFailureMessage extends VoltMessage
    {
        final HashSet<SiteFailureFault> m_failedSites;
        ExecutionSiteNodeFailureMessage(HashSet<SiteFailureFault> failedSites)
        {
            m_failedSites = failedSites;
            m_sourceHSId = m_siteId;
        }

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }

        @Override
        protected void initFromBuffer(ByteBuffer buf)
        {
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf)
        {
        }
    }

    /**
     * Generated when a snapshot buffer is discarded. Reminds the EE thread
     * that there is probably more snapshot work to do.
     */
    private class PotentialSnapshotWorkMessage extends VoltMessage
    {
        public PotentialSnapshotWorkMessage() {
            m_sourceHSId = m_siteId;
        }

        @Override
        public byte getSubject() {
            return Subject.DEFAULT.getId();
        }

        @Override
        protected void initFromBuffer(ByteBuffer buf)
        {
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf)
        {
        }
    }

    // This message is used locally to get the currently active TransactionState
    // to check whether or not its WorkUnit's dependencies have been satisfied.
    // Necessary after handling a node failure.
    static class CheckTxnStateCompletionMessage extends VoltMessage
    {
        final long m_txnId;
        CheckTxnStateCompletionMessage(long txnId, long siteId)
        {
            m_txnId = txnId;
            m_sourceHSId = siteId;
        }

        @Override
        protected void initFromBuffer(ByteBuffer buf)
        {
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf)
        {
        }
    }

    private class ExecutionSiteNodeFailureFaultHandler implements FaultHandler
    {
        /** Remember the complete set of all faulted sites */
        protected Set<Long> m_failedHsids = new HashSet<Long>();

        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            if (m_shouldContinue == false) {
                return;
            }
            HashSet<SiteFailureFault> failedSites = new HashSet<SiteFailureFault>();
            for (VoltFault fault : faults) {
                if (fault instanceof SiteFailureFault)
                {
                    SiteFailureFault site_fault = (SiteFailureFault)fault;
                    failedSites.add(site_fault);
                    // record the failed site ids for future queries on this object
                    m_failedHsids.addAll(site_fault.getSiteIds());
                }
            }
            if (!failedSites.isEmpty()) {
                m_mailbox.deliver(new ExecutionSiteNodeFailureMessage(failedSites));
            }
        }

        /**
         * Was the given site id in the log of all witnessed faults?
         */
        public boolean isWitnessedFailedSite(long hsid) {
            return m_failedHsids.contains(hsid);
        }
    }

    public boolean isActiveOrPreviouslyKnownSiteId(long hsid) {
        // if the host id is less than this one, then it existed when this site
        // was created (or it failed before this site existed)
        // This relies on the non-resuse and monotonically increasingness of host ids
        // this also assumes no garbage input
        if (CoreUtils.getHostIdFromHSId(hsid) <= CoreUtils.getHostIdFromHSId(m_siteId)) {
            return true;
        }

        // check whether this site has witnessed a failure
        if (m_faultHandler.isWitnessedFailedSite(hsid)) {
            return true;
        }

        // check if it's a live site
        if (m_tracker.getAllSites().contains(hsid)) {
            return true;
        }

        // this case should point to this id belonging to a currently joining node,
        // whose status has just not been reflected in the local tracker yet.
        return false;
    }

    /**
     * Log settings changed. Signal EE to update log level.
     */
    @Override
    public void updateBackendLogLevels() {
        ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
    }

    void startShutdown() {
        m_shouldContinue = false;
    }

    /**
     * Shutdown all resources that need to be shutdown for this <code>ExecutionSite</code>.
     * May be called twice if recursing via recursableRun(). Protected against that..
     */
    public void shutdown() {
        if (haveShutdownAlready) {
            return;
        }
        haveShutdownAlready = true;
        m_shouldContinue = false;

        boolean finished = false;
        while (!finished) {
            try {
                // Forget the m_partitionDrGateway. InvocationBufferServer
                // will be shutdown after all sites have terminated.
                m_partitionDRGateway = null;

                if (hsql != null) {
                    HsqlBackend.shutdownInstance();
                }
                if (ee != null) {
                    ee.release();
                }
                finished = true;
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            m_snapshotter.shutdown();
        } catch (InterruptedException e) {
            hostLog.warn("Interrupted during shutdown", e);
        }
    }

    /**
     * Passed to recovery processors which forward non-recovery messages to this handler.
     * Also used when recovery is enabled and there is no recovery processor for messages
     * received once the priority queue is initialized and returning txns. It is necessary
     * to do the special prehandling in this handler where txnids that are earlier then what
     * has been released from the queue during recovery because multi-part txns can involve
     * the recovering partition after the queue has already released work after the multi-part txn.
     * The recovering partition was going to give an empty responses anyways so it is fine to do
     * that in this message handler.
     */
    private final MessageHandler m_recoveryMessageHandler = new MessageHandler() {
        @Override
        public void handleMessage(VoltMessage message, long txnId) {
            if (message instanceof TransactionInfoBaseMessage) {
                long noticeTxnId = ((TransactionInfoBaseMessage)message).getTxnId();
                /**
                 * If the recovery processor and by extension this site receives
                 * a message regarding a txnid < the current supplied txnId then
                 * the message is for a multi-part txn that this site is a member of
                 * but doesn't have any info for. Send an ack with no extra processing.
                 */
                if (noticeTxnId < txnId) {
                    if (message instanceof CompleteTransactionMessage) {
                        CompleteTransactionMessage complete = (CompleteTransactionMessage)message;
                        CompleteTransactionResponseMessage ctrm =
                            new CompleteTransactionResponseMessage(complete, m_siteId);
                        m_mailbox.send(complete.getCoordinatorHSId(), ctrm);
                    } else if (message instanceof FragmentTaskMessage) {
                        FragmentTaskMessage ftask = (FragmentTaskMessage)message;
                        FragmentResponseMessage response = new FragmentResponseMessage(ftask, m_siteId);
                        response.setRecovering(true);
                        response.setStatus(FragmentResponseMessage.SUCCESS, null);

                        // add a dummy table for all of the expected dependency ids
                        for (int i = 0; i < ftask.getFragmentCount(); i++) {
                            response.addDependency(ftask.getOutputDepId(i),
                                    new VoltTable(new VoltTable.ColumnInfo("DUMMY", VoltType.BIGINT)));
                        }

                        m_mailbox.send(response.getDestinationSiteId(), response);
                    } else {
                        handleMailboxMessageNonRecursable(message);
                    }
                } else {
                    handleMailboxMessageNonRecursable(message);
                }
            } else {
                handleMailboxMessageNonRecursable(message);
            }

        }
    };

    /**
     * This is invoked after all recovery data has been received/sent. The processor can be nulled out for GC.
     */
    private final Runnable m_onRejoinCompletion = new Runnable() {
        @Override
        public void run() {
            final long now = System.currentTimeMillis();
            final boolean liveRejoin = m_recoveryProcessor == null;
            long transferred = 0;
            if (m_recoveryProcessor != null) {
                transferred = m_recoveryProcessor.bytesTransferred();
            } else {
                transferred = m_rejoinSnapshotBytes;
            }
            final long bytesTransferredTotal = m_recoveryBytesTransferred.addAndGet(transferred);
            final long megabytes = transferred / (1024 * 1024);
            final double megabytesPerSecond = megabytes / ((now - m_recoveryStartTime) / 1000.0);
            if (liveRejoin) {
                /*
                 * The logged txn count will be greater than the replayed txn count
                 * because some logged ones were before the stream snapshot
                 */
                final long duration = (System.currentTimeMillis() - m_taskExeStartTime) / 1000;
                final long throughput = duration == 0 ? m_executedTaskCount : m_executedTaskCount / duration;
                m_rejoinLog.info("Logged " + m_loggedTaskCount + " tasks");
                m_rejoinLog.info("Executed " + m_executedTaskCount + " tasks in " +
                        duration + " seconds at a rate of " +
                        throughput + " tasks/second");
            }
            m_recoveryProcessor = null;
            m_rejoinSnapshotProcessor = null;
            m_rejoinSnapshotTxnId = -1;
            m_rejoinSnapshotFinished = false;
            m_rejoinTaskLog = null;
            m_rejoining = false;
            if (m_haveRecoveryPermit) {
                m_haveRecoveryPermit = false;
                /*
                 * If it's not using pauseless rejoin, no need to release the
                 * permit here because it was never set. Pauseless rejoin has
                 * its own coordinator that makes sure only one site is doing
                 * snapshot streaming at any point of time.
                 */
                if (!liveRejoin) {
                    m_recoveryPermit.release();
                }
                m_rejoinLog.info(
                        "Destination rejoin complete for site " +
                        CoreUtils.hsIdToString(m_siteId) +
                        " partition " + m_tracker.getPartitionForSite(m_siteId) +
                        " after " + ((now - m_recoveryStartTime) / 1000) + " seconds " +
                        " with " + megabytes + " megabytes transferred " +
                        " at a rate of " + megabytesPerSecond + " megabytes/sec");
                int remaining = SnapshotSaveAPI.recoveringSiteCount.decrementAndGet();
                if (remaining == 0) {
                    ee.toggleProfiler(0);

                    /*
                     * If it's the new rejoin code, the rejoin coordinator
                     * handles this.
                     */
                    if (!liveRejoin) {
                        VoltDB.instance().onExecutionSiteRejoinCompletion(bytesTransferredTotal);
                    }
                }

                /*
                 * New rejoin is site independent, so don't have to look at the
                 * remaining count
                 */
                if (liveRejoin) {
                    // Notify the rejoin coordinator that this site has finished
                    if (m_rejoinCoordinatorHSId != -1) {
                        RejoinMessage msg =
                                new RejoinMessage(getSiteId(), RejoinMessage.Type.REPLAY_FINISHED);
                        m_mailbox.send(m_rejoinCoordinatorHSId, msg);
                    }
                    m_rejoinCoordinatorHSId = -1;
                }
            } else {
                m_rejoinLog.info("Source recovery complete for site " + m_siteId +
                        " partition " + m_tracker.getPartitionForSite(m_siteId) +
                        " after " + ((now - m_recoveryStartTime) / 1000) + " seconds " +
                        " with " + megabytes + " megabytes transferred " +
                        " at a rate of " + megabytesPerSecond + " megabytes/sec");
            }
        }
    };

    @Override
    public void tick() {
        /*
         * poke the PartitionDRGateway regularly even if we are not idle. In the
         * case where we only have multipart work to do and we are not the
         * coordinator, we still need to send heartbeat buffers.
         *
         * If the last seen txnId is larger than the current txnId, use the
         * current txnId, or otherwise we'll end up closing a buffer
         * prematurely.
         *
         * If the txnId is from before the process started, caused by command
         * log replay, then ignore it.
         */

        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        if ((time - lastTickTime) >= 1000) {
            if ((lastTickTime != 0) && (ee != null)) {
                ee.tick(time, lastCommittedTxnId);
            }
            lastTickTime = time;
        }

        // do other periodic work
    }

    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     */
    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        @Override
        public Database getDatabase()                           { return m_context.database; }
        @Override
        public Cluster getCluster()                             { return m_context.cluster; }

        /*
         * Pre-iv2 the transaction id and sp handle are absolutely always the same.
         * This is because there is a global order. In IV2 there is no global order
         * so there is a per partition order/txn-id called SpHandle which may/may not
         * be the same as the txn-id for a given transaction. If the transaction
         * is multi-part then the txnid and SpHandle will not be the same.
         */
        @Override
        public long getLastCommittedSpHandle()                     { return lastCommittedTxnId; }
        @Override
        public long getCurrentTxnId()                           { return m_currentTransactionState.txnId; }
        @Override
        public long getNextUndo()                               { return getNextUndoToken(); }
        @Override
        public ImmutableMap<String, ProcedureRunner> getProcedures() { return m_loadedProcedures.procs; }
        @Override
        public long getSiteId()                                 { return m_siteId; }
        @Override
        public boolean isLowestSiteId()                         { return m_siteId == m_tracker.getLowestSiteForHost(getHostId()); }
        @Override
        public int getHostId()                                  { return SiteTracker.getHostForSite(m_siteId); }
        @Override
        public int getPartitionId()                             { return m_tracker.getPartitionForSite(m_siteId); }
        @Override
        public long getCatalogCRC()                             { return m_context.getCatalogCRC(); }
        @Override
        public int getCatalogVersion()                          { return m_context.catalogVersion; }
        @Override
        public SiteTracker getSiteTracker()                     { return m_tracker; }
        @Override
        public SiteTracker getSiteTrackerForSnapshot()          { return m_tracker; }
        @Override
        public int getNumberOfPartitions()                      { return m_tracker.m_numberOfPartitions; }

        @Override
        public void setNumberOfPartitions(int partitionCount)
        {
            throw new UnsupportedOperationException("Changing partition count in legacy is not " +
                    "supported");
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            return ExecutionSite.this;
        }
        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            return ExecutionSite.this;
        }
        @Override
        public void updateBackendLogLevels()
        {
            ExecutionSite.this.updateBackendLogLevels();
        }
        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp, boolean requiresSnapshotIsolation)
        {
            return ExecutionSite.this.updateCatalog(diffCmds, context, csp, requiresSnapshotIsolation);
        }

        @Override
        public void updateHashinator(Pair<TheHashinator.HashinatorType, byte[]> config)
        {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    SystemProcedureContext m_systemProcedureContext;

    /**
     * Dummy ExecutionSite useful to some tests that require Mock/Do-Nothing sites.
     * @param siteId
     */
    ExecutionSite(long siteId) {
        m_siteId = siteId;
        m_systemProcedureContext = new SystemProcedureContext();
        ee = null;
        hsql = null;
        m_loadedProcedures = new LoadedProcedureSet(this, null, m_siteId, siteIndex);
        m_snapshotter = null;
        m_mailbox = null;

        // initialize the DR gateway
        m_partitionDRGateway = new PartitionDRGateway(false);
    }

    ExecutionSite(VoltDBInterface voltdb, Mailbox mailbox,
            String serializedCatalog,
            boolean recovering,
            NodeDRGateway nodeDRGateway,
            final long txnId,
            int configuredNumberOfPartitions,
            CatalogSpecificPlanner csp) throws Exception
    {
        this(voltdb, mailbox, serializedCatalog,
             new ProcedureRunnerFactory(), recovering,
             nodeDRGateway, txnId, configuredNumberOfPartitions, csp);
    }

    ExecutionSite(VoltDBInterface voltdb, Mailbox mailbox,
                  String serializedCatalogIn,
                  ProcedureRunnerFactory runnerFactory,
                  boolean recovering,
                  NodeDRGateway nodeDRGateway,
                  final long txnId,
                  int configuredNumberOfPartitions,
                  CatalogSpecificPlanner csp) throws Exception
    {
        m_siteId = mailbox.getHSId();
        hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_Initializing.name(),
                new Object[] { String.valueOf(m_siteId) }, null);

        m_context = voltdb.getCatalogContext();
        m_tracker = null;//VoltDB.instance().getSiteTracker();
        final int partitionId = m_tracker.getPartitionForSite(m_siteId);
        String txnlog_name = ExecutionSite.class.getName() + "." + m_siteId;
        m_txnlog = new VoltLogger(txnlog_name);
        m_rejoining = recovering;
        //lastCommittedTxnId = txnId;

        VoltDB.instance().getFaultDistributor().
        registerFaultHandler(SiteFailureFault.SITE_FAILURE_EXECUTION_SITE,
                             m_faultHandler,
                             FaultType.SITE_FAILURE);

        // initialize the DR gateway
        m_partitionDRGateway =
            PartitionDRGateway.getInstance(partitionId, nodeDRGateway, false, m_rejoining);

        if (voltdb.getBackendTargetType() == BackendTarget.NONE) {
            ee = new MockExecutionEngine(null);
            hsql = null;
        }
        else if (voltdb.getBackendTargetType() == BackendTarget.HSQLDB_BACKEND) {
            hsql = HsqlBackend.initializeHSQLBackend(m_siteId, m_context);
            ee = new MockExecutionEngine(null);
        }
        else {
            String serializedCatalog = serializedCatalogIn;
            if (serializedCatalog == null) {
                serializedCatalog = voltdb.getCatalogContext().catalog.serialize();
            }
            hsql = null;
            ee =
                    initializeEE(
                            voltdb.getBackendTargetType(),
                            serializedCatalog,
                            txnId,
                            m_context.m_uniqueId,
                            configuredNumberOfPartitions);
        }

        m_systemProcedureContext = new SystemProcedureContext();
        m_mailbox = mailbox;

        // setup the procedure runner wrappers.
        if (runnerFactory != null) {
            runnerFactory.configure(this, m_systemProcedureContext);
        }
        m_loadedProcedures = new LoadedProcedureSet(this, runnerFactory, getSiteId(), siteIndex);
        m_loadedProcedures.loadProcedures(m_context, voltdb.getBackendTargetType(), csp);

        int snapshotPriority = 6;
        if (m_context.cluster.getDeployment().get("deployment") != null) {
            snapshotPriority = m_context.cluster.getDeployment().get("deployment").
                getSystemsettings().get("systemsettings").getSnapshotpriority();
        }
        m_snapshotter = null;
    }

    private ExecutionEngine
    initializeEE(
            BackendTarget target,
            String serializedCatalog,
            final long txnId,
            final long timestamp,
            int configuredNumberOfPartitions)
    {
        // ExecutionSite is dead code!
        return null;
    }

    public boolean updateClusterState() {
        return true;
    }

    public boolean updateCatalog(String catalogDiffCommands, CatalogContext context,
            CatalogSpecificPlanner csp, boolean requiresSnapshotIsolation)
    {
        m_context = context;
        m_loadedProcedures.loadProcedures(m_context, VoltDB.getEEBackendType(), csp);

        //Necessary to quiesce before updating the catalog
        //so export data for the old generation is pushed to Java.
        ee.quiesce(lastCommittedTxnId);
        ee.updateCatalog( context.m_uniqueId, catalogDiffCommands);

        return true;
    }

    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        // enumerate site id (pad to 4 digits for sort)
        String name = "ExecutionSite: ";
        name += CoreUtils.hsIdToString(getSiteId());
        Thread.currentThread().setName(name);

        try {
            // Only poll messaging layer if necessary. Allow the poll
            // to block if the execution site is truly idle.
            while (m_shouldContinue) {
                TransactionState currentTxnState = null;
                m_currentTransactionState = currentTxnState;
                if (currentTxnState == null) {
                    // poll the messaging layer for a while as this site has nothing to do
                    // this will likely have a message/several messages immediately in a heavy workload
                    // Before blocking record the starvation
                    VoltMessage message = m_mailbox.recv();
                    if (message == null) {
                        //Will return null if there is no work, safe to block on the mailbox if there is no work
                        boolean hadWork =
                            (m_snapshotter.doSnapshotWork(m_systemProcedureContext,
                                    ee) != null);

                        /*
                         * Do rejoin work here before it blocks on the mailbox
                         * so that it can rejoin quickly without interrupting
                         * load too much.
                         *
                         * Rejoin and snapshot should never happen at the same
                         * time on a rejoining node, so it's fine to assign the
                         * value to hadWork here.
                         */
                        hadWork = doRejoinWork();
                        if (hadWork) {
                            continue;
                        } else {
                            message = m_mailbox.recvBlocking(5);
                        }
                    }

                    // do periodic work
                    tick();
                    if (message != null) {
                        handleMailboxMessage(message);
                    } else {
                        //idle, do snapshot work
                        m_snapshotter.doSnapshotWork(m_systemProcedureContext, ee);
                        // do some rejoin work
                        doRejoinWork();
                    }
                }
            }
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        shutdown();
    }

    /**
     * Do rejoin work, including streaming snapshot blocks and replaying logged
     * transactions.
     *
     * @return true if there was real work done.
     */
    private boolean doRejoinWork() {
        boolean doneWork = false;

        /*
         * Wait until we know the txnId of the rejoin snapshot, then start
         * restoring the snapshot blocks. When the snapshot transfer is over,
         * the snapshot processor will be set to null. If the task log is not
         * null, replay any transactions logged.
         */
        if (m_rejoinSnapshotProcessor != null && m_rejoinSnapshotTxnId != -1) {
            doneWork = restoreSnapshotForRejoin();
        } else if (m_rejoinSnapshotProcessor == null && m_rejoinTaskLog != null) {
            /*
             * snapshot streaming is done, try to replay a batch of transactions
             * to speed up the rejoin process. it should be really fast.
             */
            for (int i = 0; i < 1000; i++) {
                doneWork = replayTransactionForRejoin();
                if (!doneWork) {
                    // no more work to do for now
                    break;
                }
            }

            checkTaskExecutionProgress();
        }

        return doneWork;
    }

    /**
     * Check if the site is executing tasks faster than they come in. If the
     * site cannot keep up in a certain period of time, break rejoin.
     */
    private void checkTaskExecutionProgress() {
        final long remainingTasks = m_loggedTaskCount - m_executedTaskCount;
        final long currTime = System.currentTimeMillis();
        if (m_lastTimeMadeProgress == 0 || remainingTasks < m_remainingTasks) {
            m_lastTimeMadeProgress = currTime;
        }
        m_remainingTasks = remainingTasks;

        if (currTime > (m_lastTimeMadeProgress + MAX_BEHIND_DURATION)) {
            int duration = (int) (currTime - m_lastTimeMadeProgress) / 1000;
            m_rejoinLog.debug("Current remaining task is " + m_remainingTasks +
                                " snapshot finished " + m_rejoinSnapshotFinished);
            VoltDB.crashLocalVoltDB("Site " + CoreUtils.hsIdToString(getSiteId()) +
                                    " has not made any progress in " + duration +
                                    " seconds, please reduce workload and " +
                                    "try live rejoin again, or use " +
                                    "blocking rejoin",
                                    false, null);
        }
    }

    /**
     * Restore snapshot blocks streamed from other site if there are any.
     *
     * @return true if there was real work done.
     */
    private boolean restoreSnapshotForRejoin() {
        boolean doneWork = false;
        Pair<Integer, ByteBuffer> rejoinWork = m_rejoinSnapshotProcessor.poll(new CachedByteBufferAllocator());
        if (rejoinWork != null) {
            int tableId = rejoinWork.getFirst();
            ByteBuffer buffer = rejoinWork.getSecond();
            VoltTable table =
                    PrivateVoltTableFactory.createVoltTableFromBuffer(buffer.duplicate(),
                                                                      true);
            //m_recoveryLog.info("table " + tableId + ": " + table.toString());
            loadTable(m_rejoinSnapshotTxnId, tableId, table, false);
            doneWork = true;
        } else if (m_rejoinSnapshotProcessor.isEOF()) {
            m_rejoinLog.debug("Rejoin snapshot transfer is finished");
            m_rejoinSnapshotProcessor.close();
            m_rejoinSnapshotBytes = m_rejoinSnapshotProcessor.bytesTransferred();
            m_rejoinSnapshotProcessor = null;
            m_taskExeStartTime = System.currentTimeMillis();
            /*
             * Don't notify the rejoin coordinator yet. The stream snapshot may
             * have not finished on all nodes, let the snapshot completion
             * monitor tell the rejoin coordinator.
             */
        }

        return doneWork;
    }

    /**
     * Replays transactions logged for rejoin since the stream snapshot was
     * initiated.
     *
     * @return true if actual work was done, false otherwise
     */
    private boolean replayTransactionForRejoin() {
        return false;
    }

    /**
     * Construct a stream snapshot receiver and initiate rejoin snapshot.
     */
    private void initiateRejoin(long rejoinCoordinatorHSId) {
        m_rejoinCoordinatorHSId = rejoinCoordinatorHSId;

        // Set rejoin permit
        m_haveRecoveryPermit = true;
        m_recoveryStartTime = System.currentTimeMillis();

        // Construct a snapshot stream receiver
        m_rejoinSnapshotProcessor = new StreamSnapshotSink();

        long hsId = m_rejoinSnapshotProcessor.initialize(1, null);

        // Construct task log and start logging task messages
        int partition = getCorrespondingPartitionId();
        File overflowDir = new File(VoltDB.instance().getCatalogContext().cluster.getVoltroot(),
                                    "rejoin_overflow");
        Class<?> taskLogKlass =
                MiscUtils.loadProClass("org.voltdb.rejoin.TaskLogImpl",
                                       "Rejoin", false);
        Constructor<?> taskLogConstructor;
        try {
            taskLogConstructor = taskLogKlass.getConstructor(int.class, File.class, boolean.class);
            m_rejoinTaskLog = (TaskLog) taskLogConstructor.newInstance(partition, overflowDir, false);
        } catch (InvocationTargetException e) {
            VoltDB.crashLocalVoltDB("Unable to construct rejoin task log",
                                    true, e.getCause());
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to construct rejoin task log",
                                    true, e);
        }

        m_rejoinLog.info("Initiating rejoin for site " +
                CoreUtils.hsIdToString(getSiteId()));
        initiateRejoinSnapshot(hsId);
    }

    /**
     * Try to request a stream snapshot.
     *
     * @param hsId The HSId of the stream snapshot destination
     */
    private RejoinMessage initiateRejoinSnapshot(long hsId) {
        // Pick a replica of the same partition to send us data
        int partition = getCorrespondingPartitionId();
        long sourceSite = 0;
        List<Long> sourceSites = new ArrayList<Long>(m_tracker.getSitesForPartition(partition));
        // Order the sites by host ID so that we won't get one that's still rejoining
        TreeMap<Integer, Long> orderedSourceSites = new TreeMap<Integer, Long>();
        for (long HSId : sourceSites) {
            orderedSourceSites.put(CoreUtils.getHostIdFromHSId(HSId), HSId);
        }
        orderedSourceSites.remove(CoreUtils.getHostIdFromHSId(getSiteId()));
        if (!orderedSourceSites.isEmpty()) {
            sourceSite = orderedSourceSites.pollFirstEntry().getValue();
        } else {
            VoltDB.crashLocalVoltDB("No source for partition " + partition,
                                    false, null);
        }

        // Initiate a snapshot with stream snapshot target
        String data = null;
        try {
            JSONStringer jsStringer = new JSONStringer();
            jsStringer.object();
            jsStringer.key("streamPairs");
            jsStringer.object();
            jsStringer.key(Long.toString(sourceSite)).value(Long.toString(hsId));
            jsStringer.endObject();
            // make this snapshot only contain data from this site
            m_rejoinLog.info("Rejoin source for site " + CoreUtils.hsIdToString(getSiteId()) +
                               " is " + CoreUtils.hsIdToString(sourceSite));
            jsStringer.endObject();
            data = jsStringer.toString();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to serialize to JSON", true, e);
        }

        /*
         * The handler will be called when a snapshot request response comes
         * back. It could potentially take a long time to successfully queue the
         * snapshot request, or it may fail.
         */
        SnapshotResponseHandler handler = new SnapshotResponseHandler() {
            @Override
            public void handleResponse(ClientResponse resp) {
                if (resp == null) {
                    VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot",
                                            false, null);
                    // Prevent potential null warnings below.
                    return;
                } else if (resp.getStatus() != ClientResponseImpl.SUCCESS) {
                    VoltDB.crashLocalVoltDB("Failed to initiate rejoin snapshot: " +
                            resp.getStatusString(), false, null);
                }

                VoltTable[] results = resp.getResults();
                if (SnapshotUtil.didSnapshotRequestSucceed(results)) {
                    if (SnapshotUtil.isSnapshotQueued(results)) {
                        m_rejoinLog.debug("Rejoin snapshot queued, waiting...");
                        return;
                    }

                    long txnId = -1;
                    String appStatus = resp.getAppStatusString();
                    if (appStatus == null) {
                        VoltDB.crashLocalVoltDB("Rejoin snapshot request failed: " +
                                resp.getStatusString(), false, null);
                    }

                    try {
                        JSONObject jsObj = new JSONObject(appStatus);
                        txnId = jsObj.getLong("txnId");
                    } catch (JSONException e) {
                        VoltDB.crashLocalVoltDB("Failed to get the rejoin snapshot txnId",
                                                true, e);
                        return;
                    }

                    m_rejoinLog.debug("Received rejoin snapshot txnId " + txnId);

                    // Send a message to self to avoid synchronization
                    RejoinMessage msg = new RejoinMessage(txnId);
                    m_mailbox.send(getSiteId(), msg);
                } else {
                    VoltDB.crashLocalVoltDB("Snapshot request for rejoin failed",
                                            false, null);
                }
            }
        };

        String nonce = "Rejoin_" + getSiteId() + "_" + System.currentTimeMillis();
        SnapshotUtil.requestSnapshot(0l, "", nonce, false,
                                     SnapshotFormat.STREAM, data, handler, true);

        return null;
    }

    /**
     * Handle rejoin message for live rejoin, not blocking rejoin
     * @param rm
     */
    private void handleRejoinMessage(RejoinMessage rm) {
        Type type = rm.getType();
        if (type == RejoinMessage.Type.INITIATION) {
            // rejoin coordinator says go ahead
            initiateRejoin(rm.m_sourceHSId);
        } else if (type == RejoinMessage.Type.REQUEST_RESPONSE) {
            m_rejoinSnapshotTxnId = rm.getSnapshotTxnId();
            if (m_rejoinTaskLog != null) {
                m_rejoinTaskLog.setEarliestTxnId(m_rejoinSnapshotTxnId);
            }
            VoltDB.instance().getSnapshotCompletionMonitor()
                  .addInterest(m_snapshotCompletionHandler);
        } else {
            VoltDB.crashLocalVoltDB("Unknown rejoin message type " + type,
                                    false, null);
        }
    }

    /**
     * Run the execution site execution loop, for tests currently.
     * Will integrate this in to the real run loop soon.. ish.
     */
    public void runLoop(boolean loopUntilPoison) {
        while (m_shouldContinue) {
            TransactionState currentTxnState = null;
            m_currentTransactionState = currentTxnState;
            if (currentTxnState == null) {
                // poll the messaging layer for a while as this site has nothing to do
                // this will likely have a message/several messages immediately in a heavy workload
                VoltMessage message = m_mailbox.recv();
                tick();
                if (message != null) {
                    handleMailboxMessage(message);
                }
                else if (!loopUntilPoison){
                    // Terminate run loop on empty mailbox AND no currentTxnState
                    return;
                }
            }
        }
    }

    private void handleMailboxMessage(VoltMessage message) {
        if (m_rejoining == true && m_recoveryProcessor == null && m_currentTransactionState != null) {
            m_recoveryMessageHandler.handleMessage(message, m_currentTransactionState.txnId);
        } else {
            handleMailboxMessageNonRecursable(message);
        }
    }

    private void handleMailboxMessageNonRecursable(VoltMessage message)
    {
        /*
         * Don't listen to messages from unknown sources. The expectation is that they are from beyond
         * the grave
         */
        if (!m_tracker.m_allSitesImmutable.contains(message.m_sourceHSId)) {
            hostLog.warn("Dropping message " + message + " because it is from a unknown site id " +
                    CoreUtils.hsIdToString(message.m_sourceHSId));
            return;
        }
        if (message instanceof TransactionInfoBaseMessage) {
            TransactionInfoBaseMessage info = (TransactionInfoBaseMessage)message;
            assertTxnIdOrdering(info);

            // Special case heartbeats which only update RPQ
            if (info instanceof HeartbeatMessage) {
                return;
            }
            else if (info instanceof InitiateTaskMessage) {
            }
            //Participant notices are sent enmasse from the initiator to multiple partitions
            // and don't communicate any information about safe replication, hence DUMMY_LAST_SEEN_TXN_ID
            // it can be used for global ordering since it is a valid txnid from an initiator
            else if (info instanceof MultiPartitionParticipantMessage) {
            }

            // Every non-heartbeat notice requires a transaction state.
            TransactionState ts = m_transactionsById.get(info.getTxnId());

            if (ts != null)
            {
                if (message instanceof FragmentTaskMessage) {
                    ts.createLocalFragmentWork((FragmentTaskMessage)message, false);
                }
            }
        } else if (message instanceof RecoveryMessage) {
            final RecoveryMessage rm = (RecoveryMessage)message;
            if (rm.recoveryMessagesAvailable()) {
                return;
            }
            assert(!m_rejoining);

            /*
             * Recovery site processor hasn't been cleaned up from the previous
             * rejoin. New rejoin request cannot be processed now. Telling the
             * rejoining site to retry later.
             */
            if (m_recoveryProcessor != null || m_rejoinSnapshotProcessor != null) {
                m_rejoinLog.error("ExecutionSite is not ready to handle " +
                        "recovery request from site " +
                        CoreUtils.hsIdToString(rm.sourceSite()));
                RecoveryMessage recoveryResponse = new RecoveryMessage(false);
                m_mailbox.send(rm.sourceSite(), recoveryResponse);
                return;
            }

            final long recoveringPartitionTxnId = rm.txnId();
            m_recoveryStartTime = System.currentTimeMillis();
            m_rejoinLog.info(
                    "Recovery initiate received at site " + CoreUtils.hsIdToString(m_siteId) +
                    " from site " + CoreUtils.hsIdToString(rm.sourceSite()) + " requesting recovery start before txnid " +
                    recoveringPartitionTxnId);

            m_recoveryProcessor = RecoverySiteProcessorSource.createProcessor(
                        this,
                        rm,
                        m_context.database,
                        m_tracker,
                        ee,
                        m_mailbox,
                        m_siteId,
                        m_onRejoinCompletion,
                        m_recoveryMessageHandler);
        }
        else if (message instanceof RejoinMessage) {
            RejoinMessage rm = (RejoinMessage) message;
            handleRejoinMessage(rm);
        }
        else if (message instanceof ExecutionSiteNodeFailureMessage) {
        }
        else if (message instanceof CheckTxnStateCompletionMessage) {
            long txn_id = ((CheckTxnStateCompletionMessage)message).m_txnId;
            TransactionState txnState = m_transactionsById.get(txn_id);
            if (txnState != null)
            {
            }
        }
        else if (message instanceof RawProcessor.ExportInternalMessage) {
            RawProcessor.ExportInternalMessage exportm =
                (RawProcessor.ExportInternalMessage) message;
            ee.exportAction(exportm.m_m.isSync(),
                                exportm.m_m.getAckOffset(),
                                0,
                                exportm.m_m.getPartitionId(),
                                exportm.m_m.getSignature());
        } else if (message instanceof PotentialSnapshotWorkMessage) {
            m_snapshotter.doSnapshotWork(m_systemProcedureContext, ee);
        }
        else if (message instanceof ExecutionSiteLocalSnapshotMessage) {
            hostLog.info("Executing local snapshot. Completing any on-going snapshots.");

            // first finish any on-going snapshot
            try {
                HashSet<Exception> completeSnapshotWork = m_snapshotter.completeSnapshotWork(m_systemProcedureContext, ee);
                if (completeSnapshotWork != null && !completeSnapshotWork.isEmpty()) {
                    for (Exception e : completeSnapshotWork) {
                        hostLog.error("Error completing in progress snapshot.", e);
                    }
                }
            } catch (InterruptedException e) {
                hostLog.warn("Interrupted during snapshot completion", e);
            }

            hostLog.info("Executing local snapshot. Creating new snapshot.");

            //Flush export data to the disk before the partition detection snapshot
            ee.quiesce(lastCommittedTxnId);

            // then initiate the local snapshot
            ExecutionSiteLocalSnapshotMessage snapshotMsg =
                    (ExecutionSiteLocalSnapshotMessage) message;
            String nonce = snapshotMsg.nonce + "_" + snapshotMsg.m_roadblockTransactionId;
            SnapshotSaveAPI saveAPI = new SnapshotSaveAPI();
            VoltTable startSnapshotting = saveAPI.startSnapshotting(snapshotMsg.path,
                                      nonce,
                                      SnapshotFormat.NATIVE,
                                      (byte) 0x1,
                                      snapshotMsg.m_roadblockTransactionId,
                                      Long.MIN_VALUE,
                                      new long[0],//this param not used pre-iv2
                                      null,
                                      m_systemProcedureContext,
                                      CoreUtils.getHostnameOrAddress(),
                                      TransactionIdManager
                                          .getTimestampFromTransactionId(snapshotMsg.m_roadblockTransactionId));
            if (SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty() &&
                snapshotMsg.crash) {
                String msg = "Partition detection snapshot completed. Shutting down. " +
                        "Result: " + startSnapshotting.toString();
                VoltDB.crashLocalVoltDB(msg, false, null);
            }
        } else if (message instanceof LocalObjectMessage) {
              LocalObjectMessage lom = (LocalObjectMessage)message;
              ((Runnable)lom.payload).run();
        } else {
            hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(),
                           new Object[] { message.getClass().getName() }, null);
            VoltDB.crashLocalVoltDB("No additional info.", false, null);
        }
    }

    private void assertTxnIdOrdering(final TransactionInfoBaseMessage notice) {
        // Because of our rollback implementation, fragment tasks can arrive
        // late. This participant can have aborted and rolled back already,
        // for example.
        //
        // Additionally, commit messages for read-only MP transactions can
        // arrive after sneaked-in SP transactions have advanced the last
        // committed transaction point. A commit message is a fragment task
        // with a null payload.
        if (notice instanceof FragmentTaskMessage ||
            notice instanceof CompleteTransactionMessage)
        {
            return;
        }

        if (notice.getTxnId() < lastCommittedTxnId) {
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (DTXN) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(lastCommittedTxnId).append(" (");
            msg.append(TransactionIdManager.toString(lastCommittedTxnId)).append(" HB: ?");
            msg.append(") before\n");
            msg.append("   txn ").append(notice.getTxnId()).append(" (");
            msg.append(TransactionIdManager.toString(notice.getTxnId())).append(" HB:");
            msg.append(notice instanceof HeartbeatMessage).append(").\n");

            TransactionState txn = m_transactionsById.get(notice.getTxnId());
            if (txn != null) {
                msg.append("New notice transaction already known: " + txn.toString() + "\n");
            }
            else {
                msg.append("New notice is for new or completed transaction.\n");
            }
            msg.append("New notice of type: " + notice.getClass().getName());
            VoltDB.crashLocalVoltDB(msg.toString(), false, null);
        }

        if (notice instanceof InitiateTaskMessage) {
            InitiateTaskMessage task = (InitiateTaskMessage)notice;
            assert (task.getInitiatorHSId() != getSiteId());
        }
    }

    /*
     * When doing fault handling, it may not finish if their
     * are concurrent faults. New faults are added to this set.
     */
    private final HashSet<Long> m_pendingFailedSites = new HashSet<Long>();

    /**
     * Process a node failure detection.
     *
     * Different sites can process UpdateCatalog sysproc and handleNodeFault()
     * in different orders. UpdateCatalog changes MUST be commutative with
     * handleNodeFault.
     * @param partitionDetected
     *
     * @param siteIds Hashset<Long> of host ids of failed nodes
     * @param globalCommitPoint the surviving cluster's greatest committed multi-partition transaction id
     * @param globalInitiationPoint the greatest transaction id acknowledged as globally
     * 2PC to any surviving cluster execution site by the failed initiator.
     *
     */
    void handleSiteFaults(boolean partitionDetected,
            HashSet<Long> failedSites,
            long globalMultiPartCommitPoint,
            HashMap<Long, Long> initiatorSafeInitiationPoint)
    {
    }

    @Override
    public void initiateSnapshots(
            Deque<SnapshotTableTask> tasks,
            List<SnapshotDataTarget> targets,
            long txnId,
            int numLiveHosts,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        m_snapshotter.initiateSnapshots(ee, tasks, targets, txnId, numLiveHosts, exportSequenceNumbers);
    }

    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    @Override
    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(m_systemProcedureContext, ee);
    }


    /*
     *  SiteConnection Interface (VoltProcedure -> ExecutionSite)
     */
    @Override
    public long getCorrespondingSiteId() {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId() {
        return m_tracker.getPartitionForSite(m_siteId);
    }

    @Override
    public int getCorrespondingHostId() {
        return SiteTracker.getHostForSite(m_siteId);
    }


    @Override
    public byte[] loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data,
            boolean returnUniqueViolations)
    throws VoltAbortException
    {
        Cluster cluster = m_context.cluster;
        if (cluster == null) {
            throw new VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        Database db = cluster.getDatabases().get(databaseName);
        if (db == null) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        return loadTable(txnId, table.getRelativeIndex(), data, returnUniqueViolations);
    }

    /**
     * @param txnId
     * @param data
     * @param table
     */
    @Override
    public byte[] loadTable(long txnId, int tableId, VoltTable data, boolean returnUniqueViolations) {
        return ee.loadTable(tableId, data,
                     txnId,
                     lastCommittedTxnId,
                     returnUniqueViolations);
    }

    @Override
    public VoltTable[] executePlanFragments(
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            Object[] parameterSets,
            long txnId,//txnid is both sphandle and uniqueid pre-iv2
            long txnIdAsUniqueId,
            boolean readOnly) throws EEException
    {
        return ee.executePlanFragments(
            numFragmentIds,
            planFragmentIds,
            inputDepIds,
            parameterSets,
            txnId,
            lastCommittedTxnId,
            txnIdAsUniqueId,
            readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly) {
        if (!readOnly) {
            // pretend real work was done
            getNextUndoToken();
        }
    }

    /**
     * Continue doing runnable work for the current transaction.
     * If doWork() returns true, the transaction is over.
     * Otherwise, the procedure may have more java to run
     * or a dependency or fragment to collect from the network.
     *
     * doWork() can sneak in a new SP transaction. Maybe it would
     * be better if transactions didn't trigger other transactions
     * and those optimization decisions where made somewhere closer
     * to this code?
     */
    @Override
    public Map<Integer, List<VoltTable>>
    recursableRun(TransactionState currentTxnState)
    {
        return null;
    }

    public SiteTracker getSiteTracker() {
        return m_tracker;
    }

    /**
     * Set the txn id from the WorkUnit and set/release undo tokens as
     * necessary. The DTXN currently has no notion of maintaining undo
     * tokens beyond the life of a transaction so it is up to the execution
     * site to release the undo data in the EE up until the current point
     * when the transaction ID changes.
     */
    public void beginNewTxn(TransactionState txnState)
    {
        if (!txnState.isReadOnly()) {
            assert(txnState.getBeginUndoToken() == kInvalidUndoToken);
            txnState.setBeginUndoToken(latestUndoToken);
            assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
        }
    }

    public void rollbackTransaction(TransactionState txnState)
    {
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST rollbackTransaction " + txnState.txnId);
        }
        if (!txnState.isReadOnly()) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
            assert(latestUndoToken >= txnState.getBeginUndoToken());

            // don't go to the EE if no work was done
            if (latestUndoToken > txnState.getBeginUndoToken()) {
                ee.undoUndoToken(txnState.getBeginUndoToken());
            }
        }
    }

    public FragmentResponseMessage processFragmentTask(
            TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final VoltMessage task)
    {
        // assuming ExecutionSite is dead code
        return null;
    }

    public InitiateResponseMessage processInitiateTask(
            TransactionState txnState,
            final VoltMessage task)
    {
        final InitiateTaskMessage itask = (InitiateTaskMessage)task;
        final ProcedureRunner runner = m_loadedProcedures.procs.get(itask.getStoredProcedureName());

        final InitiateResponseMessage response = new InitiateResponseMessage(itask);

        // feasible to receive a transaction initiated with an earlier catalog.
        if (runner == null) {
            response.setResults(
                new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                       new VoltTable[] {},
                                       "Procedure does not exist: " + itask.getStoredProcedureName()));
        }
        else {
            try {
                Object[] callerParams = null;
                /*
                 * Parameters are lazily deserialized. We may not find out until now
                 * that the parameter set is corrupt
                 */
                try {
                    callerParams = itask.getParameters();
                } catch (RuntimeException e) {
                    Writer result = new StringWriter();
                    PrintWriter pw = new PrintWriter(result);
                    e.printStackTrace(pw);
                    response.setResults(
                            new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                                   new VoltTable[] {},
                                                   "Exception while deserializing procedure params\n" +
                                                   result.toString()));
                }
                if (callerParams != null) {
                    ClientResponseImpl cr = null;

                    // call the proc
                    runner.setupTransaction(txnState);
                    cr = runner.call(itask.getParameters());
                    txnState.setHash(cr.getHash());
                    response.setResults(cr);

                    // record the results of write transactions to the transaction state
                    // this may be used to verify the DR replica cluster gets the same value
                    // skip for multi-partition txns because only 1 of k+1 partitions will
                    //  have the real results
                    if ((!itask.isReadOnly()) && itask.isSinglePartition()) {
                        txnState.storeResults(cr);
                    }
                }
            }
            catch (final ExpectedProcedureException e) {
                log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ExpectedProcedureException.name(), e);
                response.setResults(
                                    new ClientResponseImpl(
                                                           ClientResponse.GRACEFUL_FAILURE,
                                                           new VoltTable[]{},
                                                           e.toString()));
            }
            catch (final Exception e) {
                // Should not be able to reach here. VoltProcedure.call caught all invocation target exceptions
                // and converted them to error responses. Java errors are re-thrown, and not caught by this
                // exception clause. A truly unexpected exception reached this point. Crash. It's a defect.
                hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_UnexpectedProcedureException.name(), e);
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }
        }
        log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        return response;
    }

    public PartitionDRGateway getPartitionDRGateway() {
        return m_partitionDRGateway;
    }

    public void notifySitesAdded(final SiteTracker st) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                if (!m_pendingFailedSites.isEmpty()) {
                    return;
                }

                /*
                 * Failure processing may pick up the site tracker eagerly
                 */
                if (st.m_version <= m_tracker.m_version){
                    return;
                }

                m_tracker = st;
            }
        };
        LocalObjectMessage lom = new LocalObjectMessage(r);
        lom.m_sourceHSId = m_siteId;
        m_mailbox.deliver(lom);
    }

    // do-nothing implementation of IV2 SiteProcedeConnection API
    @Override
    public void truncateUndoLog(boolean rollback, long token, long txnId, long spHandle) {
        throw new RuntimeException("Unsupported IV2-only API.");
    }

    // do-nothing implementation of IV2 sysproc fragment API.
    @Override
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params) {
        throw new RuntimeException("Unsupported IV2-only API.");
     }

    @Override
    public Future<?> doSnapshotWork()
    {
        throw new RuntimeException("Unsupported IV2-only API.");
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        ee.stashWorkUnitDependencies(dependencies);
    }

    @Override
    public HsqlBackend getHsqlBackendIfExists()
    {
        return hsql;
    }

    @Override
    public long[] getUSOForExportTable(String signature)
    {
        return ee.getUSOForExportTable(signature);
    }

    @Override
    public void toggleProfiler(int toggle)
    {
        ee.toggleProfiler(toggle);
    }

    @Override
    public void quiesce()
    {
        ee.quiesce(lastCommittedTxnId);
    }

    @Override
    public void exportAction(boolean syncAction,
                             long ackOffset,
                             Long sequenceNumber,
                             Integer partitionId,
                             String tableSignature)
    {
        ee.exportAction(syncAction, ackOffset, sequenceNumber, partitionId,
                        tableSignature);
    }

    @Override
    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        return ee.getStats(selector, locators, interval, now);
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction ignored,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        throw new RuntimeException("setRejoinComplete is an IV2-only interface.");
    }

    @Override
    public ProcedureRunner getProcedureRunner(String procedureName) {
        throw new RuntimeException("getProcedureRunner is an IV2-only interface.");
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds) {
        //A noop pre-IV2
    }

    @Override
    public long getFragmentIdForPlanHash(byte[] planHash) {
        //A noop pre-IV2
        return 0;
    }

    @Override
    public long loadOrAddRefPlanFragment(byte[] planHash, byte[] plan) {
        //A noop pre-IV2
        return 0;
    }

    @Override
    public void decrefPlanFragmentById(long fragmentId) {
        //A noop pre-IV2
    }

    @Override
    public byte[] planForFragmentId(long fragmentId) {
        return null;
    }

    @Override
    public long[] validatePartitioning(long[] tableIds, int hashinatorType, byte[] hashinatorConfig) {
        throw new UnsupportedOperationException();
    }
}
