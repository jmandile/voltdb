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

package org.voltdb.iv2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Future;

//import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.FragmentPlanSource;
import org.voltdb.HsqlBackend;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.Sha1Wrapper;

import com.google.common.collect.ImmutableMap;

public class MpRoSite implements Runnable, SiteProcedureConnection, FragmentPlanSource
{
    //private static final VoltLogger hostLog = new VoltLogger("HOST");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    // Partition count is important for some reason.
    int m_numberOfPartitions;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Manages pending tasks.
    final SiteTaskerQueue m_scheduler;

    /*
     * There is really no legit reason to touch the initiator mailbox from the site,
     * but it turns out to be necessary at startup when restoring a snapshot. The snapshot
     * has the transaction id for the partition that it must continue from and it has to be
     * set at all replicas of the partition.
     */
    final InitiatorMailbox m_initiatorMailbox;

    // Still need m_hsql here.
    HsqlBackend m_hsql;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedure
    volatile LoadedProcedureSet m_loadedProcedures;

    // Current topology
    int m_partitionId;

    // Undo token state for the corresponding EE.
    public final static long kInvalidUndoToken = -1L;
    long latestUndoToken = 0L;

    @Override
    public long getNextUndoToken()
    {
        return ++latestUndoToken;
    }

    @Override
    public long getLatestUndoToken()
    {
        return latestUndoToken;
    }

    // Advanced in complete transaction.
    private long m_currentTxnId = Long.MIN_VALUE;

    SiteProcedureConnection getSiteProcedureConnection()
    {
        return this;
    }

    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     */
    SystemProcedureExecutionContext m_sysprocContext = new SystemProcedureExecutionContext() {
        @Override
        public Database getDatabase() {
            return m_context.database;
        }

        @Override
        public Cluster getCluster() {
            return m_context.cluster;
        }

        @Override
        public long getLastCommittedSpHandle() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getCurrentTxnId() {
            return m_currentTxnId;
        }

        @Override
        public long getNextUndo() {
            return getNextUndoToken();
        }

        @Override
        public ImmutableMap<String, ProcedureRunner> getProcedures() {
            throw new RuntimeException("Not implemented in iv2");
            // return m_loadedProcedures.procs;
        }

        @Override
        public long getSiteId() {
            return m_siteId;
        }

        /*
         * Expensive to compute, memoize it
         */
        private Boolean m_isLowestSiteId = null;
        @Override
        public boolean isLowestSiteId()
        {
            if (m_isLowestSiteId != null) {
                return m_isLowestSiteId;
            } else {
                // FUTURE: should pass this status in at construction.
                long lowestSiteId = VoltDB.instance().getSiteTrackerForSnapshot().getLowestSiteForHost(getHostId());
                m_isLowestSiteId = m_siteId == lowestSiteId;
                return m_isLowestSiteId;
            }
        }


        @Override
        public int getHostId() {
            return CoreUtils.getHostIdFromHSId(m_siteId);
        }

        @Override
        public int getPartitionId() {
            return m_partitionId;
        }

        @Override
        public long getCatalogCRC() {
            return m_context.getCatalogCRC();
        }

        @Override
        public int getCatalogVersion() {
            return m_context.catalogVersion;
        }

        @Override
        public SiteTracker getSiteTracker() {
            throw new RuntimeException("Not implemented in iv2");
        }

        @Override
        public SiteTracker getSiteTrackerForSnapshot() {
            return VoltDB.instance().getSiteTrackerForSnapshot();
        }

        @Override
        public int getNumberOfPartitions() {
            return m_numberOfPartitions;
        }

        @Override
        public void setNumberOfPartitions(int partitionCount) {
            MpRoSite.this.setNumberOfPartitions(partitionCount);
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            return MpRoSite.this;
        }

        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void updateBackendLogLevels() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context,
                CatalogSpecificPlanner csp, boolean requiresSnapshotIsolation)
        {
            return MpRoSite.this.updateCatalog(diffCmds, context, csp, requiresSnapshotIsolation, false);
        }

        @Override
        public void updateHashinator(Pair<TheHashinator.HashinatorType, byte[]> config)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }
    };

    /** Create a new RO MP execution site */
    public MpRoSite(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            int partitionId,
            int numPartitions,
            InitiatorMailbox initiatorMailbox)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;
        m_currentTxnId = Long.MIN_VALUE;
        m_initiatorMailbox = initiatorMailbox;
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize()
    {
        if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
        }
        else {
            m_hsql = null;
        }
    }

    /** Create a native VoltDB execution engine */
    ExecutionEngine initializeEE(String serializedCatalog, final long timestamp)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void run()
    {
        Thread.currentThread().setName("Iv2ExecutionSite: " + CoreUtils.hsIdToString(m_siteId));
        initialize();

        try {
            while (m_shouldContinue) {
                // Normal operation blocks the site thread on the sitetasker queue.
                SiteTasker task = m_scheduler.take();
                if (task instanceof TransactionTask) {
                    m_currentTxnId = ((TransactionTask)task).getTxnId();
                }
                task.run(getSiteProcedureConnection());
            }
        }
        catch (OutOfMemoryError e)
        {
            // Even though OOM should be caught by the Throwable section below,
            // it sadly needs to be handled seperately. The goal here is to make
            // sure VoltDB crashes.
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " ran out of Java memory. " + "This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, true, e);
        }
        catch (Throwable t)
        {
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " encountered an " + "unexpected error and will die, taking this VoltDB node down.";
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
        shutdown();
    }

    public void startShutdown()
    {
        m_shouldContinue = false;
    }

    void shutdown()
    {
        if (m_hsql != null) {
            HsqlBackend.shutdownInstance();
        }
    }

    //
    // Legacy SiteProcedureConnection needed by ProcedureRunner
    //
    @Override
    public long getCorrespondingSiteId()
    {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId()
    {
        return m_partitionId;
    }

    @Override
    public int getCorrespondingHostId()
    {
        return CoreUtils.getHostIdFromHSId(m_siteId);
    }

    @Override
    public byte[] loadTable(long txnId, String clusterName, String databaseName,
            String tableName, VoltTable data, boolean returnUniqueViolations) throws VoltAbortException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public byte[] loadTable(long spHandle, int tableId, VoltTable data, boolean returnUniqueViolations)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void updateBackendLogLevels()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly)
    {
        throw new RuntimeException("Not supported in IV2.");
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        return currentTxnState.recursableRun(this);
    }

    @Override
    public void truncateUndoLog(boolean rollback, long beginUndoToken, long txnId, long spHandle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params)
    {
        ProcedureRunner runner = m_loadedProcedures.getSysproc(fragmentId);
        return runner.executeSysProcPlanFragment(txnState, dependencies, fragmentId, params);
    }

    @Override
    public HsqlBackend getHsqlBackendIfExists()
    {
        return m_hsql;
    }

    @Override
    public long[] getUSOForExportTable(String signature)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void toggleProfiler(int toggle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void tick()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void quiesce()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void exportAction(boolean syncAction,
                             long ackOffset,
                             Long sequenceNumber,
                             Integer partitionId, String tableSignature)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public Future<?> doSnapshotWork()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction replayComplete,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public VoltTable[] executePlanFragments(int numFragmentIds,
            long[] planFragmentIds, long[] inputDepIds,
            Object[] parameterSets, long spHandle, long uniqueId, boolean readOnly)
            throws EEException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public ProcedureRunner getProcedureRunner(String procedureName) {
        return m_loadedProcedures.getProcByName(procedureName);
    }

    /**
     * Update the catalog.  If we're the MPI, don't bother with the EE.
     */
    // IZZY-MP-RO: PLAN ON GETTING RID OF THIS
    public boolean updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp,
            boolean requiresSnapshotIsolationboolean, boolean isMPI)
    {
        m_context = context;
        m_loadedProcedures.loadProcedures(m_context, m_backend, csp);
        return true;
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds) {
        boolean foundMultipartTxnId = false;
        boolean foundSinglepartTxnId = false;
        for (long txnId : perPartitionTxnIds) {
            if (TxnEgo.getPartitionId(txnId) == m_partitionId) {
                if (foundSinglepartTxnId) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids during restore for a partition", false, null);
                }
                foundSinglepartTxnId = true;
                m_initiatorMailbox.setMaxLastSeenTxnId(txnId);
            }
            if (TxnEgo.getPartitionId(txnId) == MpInitiator.MP_INIT_PID) {
                if (foundMultipartTxnId) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids during restore for a multipart txnid", false, null);
                }
                foundMultipartTxnId = true;
                m_initiatorMailbox.setMaxLastSeenMultipartTxnId(txnId);
            }
        }
        if (!foundMultipartTxnId) {
            VoltDB.crashLocalVoltDB("Didn't find a multipart txnid on restore", false, null);
        }
    }

    public void setNumberOfPartitions(int partitionCount)
    {
        m_numberOfPartitions = partitionCount;
    }

    /// A plan fragment entry in the cache.
    private static class FragInfo {
        final Sha1Wrapper hash;
        final long fragId;
        final byte[] plan;
        int refCount;
        /// The ticker value current when this fragment was last (dis)used.
        /// A new FragInfo or any other not in the LRU map because it is being referenced has value 0.
        /// A non-zero value is either the fragment's current key in the LRU map OR its intended/future
        /// key, if it has been lazily updated after the fragment was reused.
        long lastUse;

        FragInfo(Sha1Wrapper key, byte[] plan, long nextId)
        {
            this.hash = key;
            this.plan = plan;
            this.fragId = nextId;
            this.refCount = 0;
            this.lastUse = 0;
        }

    }

    HashMap<Sha1Wrapper, FragInfo> m_plansByHash = new HashMap<Sha1Wrapper, FragInfo>();
    HashMap<Long, FragInfo> m_plansById = new HashMap<Long, FragInfo>();
    TreeMap<Long, FragInfo> m_plansLRU = new TreeMap<Long, FragInfo>();
    /// A ticker that provides temporary ids for all cached fragments, for communicating with the EE.
    long m_nextFragId = 5000;
    /// A ticker that allows the sequencing of all fragment uses, providing a key to the LRU map.
    long m_nextFragUse = 1;

    @Override
    public long getFragmentIdForPlanHash(byte[] planHash) {
        Sha1Wrapper key = new Sha1Wrapper(planHash);
        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansByHash.get(key);
        }
        assert(frag != null);
        return frag.fragId;
    }

    @Override
    public long loadOrAddRefPlanFragment(byte[] planHash, byte[] plan) {
        Sha1Wrapper key = new Sha1Wrapper(planHash);
        synchronized (FragInfo.class) {
            FragInfo frag = m_plansByHash.get(key);
            if (frag == null) {
                frag = new FragInfo(key, plan, m_nextFragId++);
                m_plansByHash.put(frag.hash, frag);
                m_plansById.put(frag.fragId, frag);
                if (m_plansById.size() > ExecutionEngine.EE_PLAN_CACHE_SIZE) {
                    evictLRUfragment();
                }
            }
            // The fragment MAY be in the LRU map.
            // An incremented refCount is a lazy way to keep it safe from eviction
            // without having to update the map.
            // This optimizes for popular fragments in a small or stable cache that may be reused
            // many times before the eviction process needs to take any notice.
            frag.refCount++;
            return frag.fragId;
        }
    }

    /// Evict the least recently used fragment (if any are currently unused).
    /// Along the way, update any obsolete entries that were left
    /// by the laziness of the fragment state changes (fragment reuse).
    /// In the rare case of a cache bloated beyond its usual limit,
    /// keep evicting as needed and as entries are available until the bloat is gone.
    void evictLRUfragment()
    {
        while ( ! m_plansLRU.isEmpty()) {
            // Remove the earliest entry.
            Entry<Long, FragInfo> lru = m_plansLRU.pollFirstEntry();
            FragInfo frag = lru.getValue();
            if (frag.refCount > 0) {
                // The fragment is being re-used, it is no longer an eviction candidate.
                // It is only in the map due to the laziness in loadOrAddRefPlanFragment.
                // It will be re-considered (at a later key) once it is no longer referenced.
                // Resetting its lastUse to 0, here, restores it to a state identical to that
                // of a new fragment.
                // It eventually causes decrefPlanFragmentById to put it back in the map
                // at its then up-to-date key.
                // This makes it safe to keep out of the LRU map for now.
                // See the comment in decrefPlanFragmentById and the one in the next code block.
                frag.lastUse = 0;
            }
            else if (lru.getKey() != frag.lastUse) {
                // The fragment is not in use but has been re-used more recently than the key reflects.
                // This is a result of the laziness in decrefPlanFragmentById.
                // Correct the entry's key in the LRU map to reflect its last use.
                // This may STILL be the least recently used entry.
                // If so, it will be picked off in a later iteration of this loop;
                // its key will now match its lastUse value.
                m_plansLRU.put(frag.lastUse, frag);
            }
            else {
                // Found and removed the actual up-to-date least recently used entry from the LRU map.
                // Remove the entry from the other collections.
                m_plansById.remove(frag.fragId);
                m_plansByHash.remove(frag.hash);
                // Normally, one eviction for each new fragment is enough to restore order.
                // BUT, if a prior call ever failed to find an unused fragment in the cache,
                // the cache may have grown beyond its normal size. In that rare case,
                // one eviction is not enough to reduce the cache to the desired size,
                // so take another bite at the apple.
                // Otherwise, trading exactly one evicted fragment for each new fragment
                // would never reduce the cache.
                if (m_plansById.size() > ExecutionEngine.EE_PLAN_CACHE_SIZE) {
                     continue;
                }
                return;
            }
        }
        // Strange. All FragInfo entries appear to be in use. There's nothing to evict.
        // Let the cache bloat a little and try again later after the next new fragment.
    }

    @Override
    public void decrefPlanFragmentById(long fragmentId) {
        // skip dummy/invalid fragment ids
        if (fragmentId <= 0) return;

        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansById.get(fragmentId);
            assert(frag != null);
            if (--frag.refCount == 0) {
                // The disused fragment belongs in the LRU map at the end -- at the current "ticker".
                // If its lastUse value is 0 like a new entry's, it is not currently in the map.
                // Put into the map in its proper position.
                // If it is already in the LRU map (at a "too early" entry), just set its lastUse value
                // as a cheap way to notify evictLRUfragment that it is not ready for eviction but
                // should instead be re-ordered further forward in the map.
                // This re-ordering only needs to happen when the eviction process considers the entry.
                // For a popular fragment in a small or stable cache, that may be after MANY
                // re-uses like this.
                // This prevents thrashing of the LRU map, repositioning recent entries.
                boolean notInLRUmap = (frag.lastUse == 0); // check this BEFORE updating lastUse
                frag.lastUse = ++m_nextFragUse;
                if (notInLRUmap) {
                    m_plansLRU.put(frag.lastUse, frag);
                }
            }
        }
    }

    /**
     * Called from the execution engine to fetch a plan for a given hash.
     */
    @Override
    public byte[] planForFragmentId(long fragmentId) {
        assert(fragmentId > 0);

        FragInfo frag = null;
        synchronized (FragInfo.class) {
            frag = m_plansById.get(fragmentId);
        }
        assert(frag != null);
        return frag.plan;
    }

    /**
     * For the specified list of table ids, return the number of mispartitioned rows using
     * the provided hashinator and hashinator config
     */
    @Override
    public long[] validatePartitioning(long[] tableIds, int hashinatorType, byte[] hashinatorConfig) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }
}
