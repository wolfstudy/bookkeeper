/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.stats.GarbageCollectorStats;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends SafeRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int SECOND = 1000;

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    // key 是 EntryLog file ID
    private Map<Long, EntryLogMetadata> entryLogMetaMap = new ConcurrentHashMap<Long, EntryLogMetadata>();

    // 驱动 GC Compactor 的线程
    private final ScheduledExecutorService gcExecutor;
    Future<?> scheduledFuture = null;

    // This is how often we want to run the Garbage Collector Thread (in milliseconds).
    final long gcWaitTime;

    // Compaction parameters
    boolean isForceMinorCompactionAllow = false;
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;
    final long minorCompactionMaxTimeMillis;
    long lastMinorCompactionTime;

    boolean isForceMajorCompactionAllow = false;
    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;
    long majorCompactionMaxTimeMillis;
    long lastMajorCompactionTime;

    @Getter
    final boolean isForceGCAllowWhenNoSpace;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    final AbstractLogCompactor compactor;

    // Stats loggers for garbage collection operations
    private final GarbageCollectorStats gcStats;

    private volatile long totalEntryLogSize;
    private volatile int numActiveEntryLogs;

    final CompactableLedgerStorage ledgerStorage;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    // use to get the compacting status
    final AtomicBoolean minorCompacting = new AtomicBoolean(false);
    final AtomicBoolean majorCompacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    // Boolean to disable major compaction, when disk is almost full
    final AtomicBoolean suspendMajorCompaction = new AtomicBoolean(false);
    // Boolean to disable minor compaction, when disk is full
    final AtomicBoolean suspendMinorCompaction = new AtomicBoolean(false);

    final ScanAndCompareGarbageCollector garbageCollector;
    final GarbageCleaner garbageCleaner;

    final ServerConfiguration conf;

    /**
     * Create a garbage collector thread.
     * 构造函数
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf, LedgerManager ledgerManager,
            final CompactableLedgerStorage ledgerStorage, StatsLogger statsLogger) throws IOException {
        // 创建一个单线程执行程序，它可安排在给定延迟后运行命令或者定期地执行任务。可保证顺序地执行各个任务，并且在任意给定的时间不会有多个线程是活动的。
        this(conf, ledgerManager, ledgerStorage, statsLogger,
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("GarbageCollectorThread")));
    }

    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf,
                                  LedgerManager ledgerManager,
                                  final CompactableLedgerStorage ledgerStorage,
                                  StatsLogger statsLogger,
                                  ScheduledExecutorService gcExecutor)
        throws IOException {
        this.gcExecutor = gcExecutor;
        this.conf = conf;

        this.entryLogger = ledgerStorage.getEntryLogger();
        this.ledgerStorage = ledgerStorage;
        // GC 线程定期执行的时间间隔
        this.gcWaitTime = conf.getGcWaitTime();

        this.numActiveEntryLogs = 0;
        this.totalEntryLogSize = 0L;
        this.garbageCollector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage, conf, statsLogger);
        this.gcStats = new GarbageCollectorStats(
            statsLogger,
            () -> numActiveEntryLogs,
            () -> totalEntryLogSize,
            () -> garbageCollector.getNumActiveLedgers()
        );

        this.garbageCleaner = ledgerId -> {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("delete ledger : " + ledgerId);
                }
                gcStats.getDeletedLedgerCounter().inc();
                ledgerStorage.deleteLedger(ledgerId);
            } catch (IOException e) {
                LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
            }
        };

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;
        isForceGCAllowWhenNoSpace = conf.getIsForceGCAllowWhenNoSpace();
        majorCompactionMaxTimeMillis = conf.getMajorCompactionMaxTimeMillis();
        minorCompactionMaxTimeMillis = conf.getMinorCompactionMaxTimeMillis();

        boolean isForceAllowCompaction = conf.isForceAllowCompaction();

        AbstractLogCompactor.LogRemovalListener remover = new AbstractLogCompactor.LogRemovalListener() {
            @Override
            public void removeEntryLog(long logToRemove) {
                GarbageCollectorThread.this.removeEntryLog(logToRemove);
            }
        };
        if (conf.getUseTransactionalCompaction()) {
            this.compactor = new TransactionalEntryLogCompactor(conf, entryLogger, ledgerStorage, remover);
        } else {
            this.compactor = new EntryLogCompactor(conf, entryLogger, ledgerStorage, remover);
        }

        if (minorCompactionInterval > 0 && minorCompactionThreshold > 0) {
            if (minorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid minor compaction threshold "
                                    + minorCompactionThreshold);
            }
            if (minorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short minor compaction interval : "
                                    + minorCompactionInterval);
            }
            enableMinorCompaction = true;
        }

        if (isForceAllowCompaction) {
            if (minorCompactionThreshold > 0 && minorCompactionThreshold < 1.0f) {
                isForceMinorCompactionAllow = true;
            }
            if (majorCompactionThreshold > 0 && majorCompactionThreshold < 1.0f) {
                isForceMajorCompactionAllow = true;
            }
        }

        if (majorCompactionInterval > 0 && majorCompactionThreshold > 0) {
            if (majorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid major compaction threshold "
                                    + majorCompactionThreshold);
            }
            if (majorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short major compaction interval : "
                                    + majorCompactionInterval);
            }
            enableMajorCompaction = true;
        }

        if (enableMinorCompaction && enableMajorCompaction) {
            if (minorCompactionInterval >= majorCompactionInterval
                || minorCompactionThreshold >= majorCompactionThreshold) {
                throw new IOException("Invalid minor/major compaction settings : minor ("
                                    + minorCompactionThreshold + ", " + minorCompactionInterval
                                    + "), major (" + majorCompactionThreshold + ", "
                                    + majorCompactionInterval + ")");
            }
        }

        LOG.info("Minor Compaction : enabled=" + enableMinorCompaction + ", threshold="
               + minorCompactionThreshold + ", interval=" + minorCompactionInterval);
        LOG.info("Major Compaction : enabled=" + enableMajorCompaction + ", threshold="
               + majorCompactionThreshold + ", interval=" + majorCompactionInterval);

        lastMinorCompactionTime = lastMajorCompactionTime = System.currentTimeMillis();
    }

    public void enableForceGC() {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}", Thread.currentThread().getName());
            triggerGC(true, suspendMajorCompaction.get(),
                      suspendMinorCompaction.get());
        }
    }

    public void disableForceGC() {
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    Future<?> triggerGC(final boolean force,
                        final boolean suspendMajor,
                        final boolean suspendMinor) {
        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    Future<?> triggerGC() {
        final boolean force = forceGarbageCollection.get();
        final boolean suspendMajor = suspendMajorCompaction.get();
        final boolean suspendMinor = suspendMinorCompaction.get();

        return gcExecutor.submit(() -> {
                runWithFlags(force, suspendMajor, suspendMinor);
            });
    }

    public boolean isInForceGC() {
        return forceGarbageCollection.get();
    }

    public void suspendMajorGC() {
        if (suspendMajorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Major Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMajorGC() {
        if (suspendMajorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Major Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void suspendMinorGC() {
        if (suspendMinorCompaction.compareAndSet(false, true)) {
            LOG.info("Suspend Minor Compaction triggered by thread: {}", Thread.currentThread().getName());
        }
    }

    public void resumeMinorGC() {
        if (suspendMinorCompaction.compareAndSet(true, false)) {
            LOG.info("{} Minor Compaction back to normal since bookie has enough space now.",
                    Thread.currentThread().getName());
        }
    }

    public void start() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = gcExecutor.scheduleAtFixedRate(this, gcWaitTime, gcWaitTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void safeRun() {
        boolean force = forceGarbageCollection.get();
        boolean suspendMajor = suspendMajorCompaction.get();
        boolean suspendMinor = suspendMinorCompaction.get();

        runWithFlags(force, suspendMajor, suspendMinor);

        if (force) {
            // only set force to false if it had been true when the garbage
            // collection cycle started
            forceGarbageCollection.set(false);
        }
    }

    public void runWithFlags(boolean force, boolean suspendMajor, boolean suspendMinor) {
        long threadStart = MathUtils.nowInNano();
        if (force) {
            LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
        }
        // Recover and clean up previous state if using transactional compaction
        compactor.cleanUpAndRecover();

        // Extract all of the ledger ID's that comprise all of the entry logs
        // (except for the current new one which is still being written to).
        entryLogMetaMap = extractMetaFromEntryLogs(entryLogMetaMap);

        // gc inactive/deleted ledgers
        doGcLedgers();

        // gc entry logs
        // 这里的行为，更多的是尝试性的先去迭代一下本地所有的 EntryLog 对象，看是否有 EntryLog 已经可以直接回收了
        doGcEntryLogs();

        if (suspendMajor) {
            LOG.info("Disk almost full, suspend major compaction to slow down filling disk.");
        }
        if (suspendMinor) {
            LOG.info("Disk full, suspend minor compaction to slow down filling disk.");
        }

        // 默认 minor 和 major GC 都开启了的，minor GC的阈值是 20%，major GC 的阈值是 80%
        // minor GC 触发的间隔为 3600 秒（1小时）
        // major GC 触发的间隔为 86400 秒（24小时）
        long curTime = System.currentTimeMillis();
        if (((isForceMajorCompactionAllow && force)
                || (enableMajorCompaction && (force || curTime - lastMajorCompactionTime > majorCompactionInterval)))
                && (!suspendMajor)) {
            // enter major compaction
            LOG.info("Enter major compaction, suspendMajor {}", suspendMajor);
            majorCompacting.set(true);
            // 回收的核心逻辑函数，majorCompactionMaxTimeMillis 默认为 -1，即要等到所有的 compactor 都扫描执行完才退出
            doCompactEntryLogs(majorCompactionThreshold, majorCompactionMaxTimeMillis);
            lastMajorCompactionTime = System.currentTimeMillis();
            // and also move minor compaction time
            lastMinorCompactionTime = lastMajorCompactionTime;
            gcStats.getMajorCompactionCounter().inc();
            majorCompacting.set(false);
        } else if (((isForceMinorCompactionAllow && force)
                || (enableMinorCompaction && (force || curTime - lastMinorCompactionTime > minorCompactionInterval)))
                && (!suspendMinor)) {
            // enter minor compaction
            LOG.info("Enter minor compaction, suspendMinor {}", suspendMinor);
            minorCompacting.set(true);
            // 回收的核心逻辑函数， minorCompactionMaxTimeMillis  默认为 -1，即要等到所有的 compactor 都扫描执行完才退出
            doCompactEntryLogs(minorCompactionThreshold, minorCompactionMaxTimeMillis);
            lastMinorCompactionTime = System.currentTimeMillis();
            gcStats.getMinorCompactionCounter().inc();
            minorCompacting.set(false);
        }

        if (force) {
            if (forceGarbageCollection.compareAndSet(true, false)) {
                LOG.info("{} Set forceGarbageCollection to false after force GC to make it forceGC-able again.", Thread
                    .currentThread().getName());
            }
        }
        gcStats.getGcThreadRuntime().registerSuccessfulEvent(
                MathUtils.nowInNano() - threadStart, TimeUnit.NANOSECONDS);
    }

    /**
     * Do garbage collection ledger index files.
     */
    private void doGcLedgers() {
        garbageCollector.gc(garbageCleaner);
    }

    /**
     * Garbage collect those entry loggers which are not associated with any active ledgers.
     */
    private void doGcEntryLogs() {
        // Get a cumulative count, don't update until complete
        AtomicLong totalEntryLogSizeAcc = new AtomicLong(0L);

        // Loop through all of the entry logs and remove the non-active ledgers.
        // entryLogMetaMap 这个 Map 记录了整个 EntryLog 从 EntryLogID 到 EntryMeta 的映射关系。
        entryLogMetaMap.forEach((entryLogId, meta) -> {
           // 先去看看当前的 EntryLog Meta 中记录的有哪些 ledgers 是可以删除的，这里操作的是 EntryLog Meta 中的 ledgersMap 对象
           removeIfLedgerNotExists(meta);

           // 判断 EntryLog Meta 中的 ledgersMap 对象是否还有元素。
           if (meta.isEmpty()) {
               // This means the entry log is not associated with any active ledgers anymore.
               // We can remove this entry log file now.
               LOG.info("Deleting entryLogId " + entryLogId + " as it has no active ledgers!");
               // 当当前的 EntryLog 中没有任何 Ledgers 对象时，直接调用删除 EntryLog 的接口进行删除操作。
               removeEntryLog(entryLogId);
               gcStats.getReclaimedSpaceViaDeletes().add(meta.getTotalSize());
           }

           totalEntryLogSizeAcc.getAndAdd(meta.getRemainingSize());
        });

        this.totalEntryLogSize = totalEntryLogSizeAcc.get();
        this.numActiveEntryLogs = entryLogMetaMap.keySet().size();
    }

    private void removeIfLedgerNotExists(EntryLogMetadata meta) {
        meta.removeLedgerIf((entryLogLedger) -> {
            // Remove the entry log ledger from the set if it isn't active.
            try {
                // ledgerStorage为专门为压缩定制的 CompactableLedgerStorage，继承了 LedgerStorage 接口
                return !ledgerStorage.ledgerExists(entryLogLedger);
            } catch (IOException e) {
                LOG.error("Error reading from ledger storage", e);
                return false;
            }
        });
    }

    /**
     * Compact entry logs if necessary.
     *
     * <p>
     * Compaction will be executed from low unused space to high unused space.
     * Those entry log files whose remaining size percentage is higher than threshold
     * would not be compacted.
     * </p>
     *
     * 根据 minor GC 和 major GC 不同的触发阈值以及最大允许的时间来处理 EntryLog 的回收
     */
    @VisibleForTesting
    void doCompactEntryLogs(double threshold, long maxTimeMillis) {
        LOG.info("Do compaction to compact those files lower than {}", threshold);

        // sort the ledger meta by usage in ascending order.
        List<EntryLogMetadata> logsToCompact = new ArrayList<EntryLogMetadata>();
        // 开始之前首先把本地缓存的 entryLogMetaMap 都添加进来
        logsToCompact.addAll(entryLogMetaMap.values());
        // 按照使用率做一个排序
        logsToCompact.sort(Comparator.comparing(EntryLogMetadata::getUsage));

        // 默认分为 10个 buckets（[10% 20% 30% 40% 50% 60% 70% 80% 90% 100%]）
        final int numBuckets = 10;
        int[] entryLogUsageBuckets = new int[numBuckets];
        int[] compactedBuckets = new int[numBuckets];

        long start = System.currentTimeMillis();
        long end = start;
        long timeDiff = 0;

        // 迭代entryLogMetaMap的临时对象：logsToCompact
        for (EntryLogMetadata meta : logsToCompact) {
            int bucketIndex = calculateUsageIndex(numBuckets, meta.getUsage());
            entryLogUsageBuckets[bucketIndex]++;

            // 更新 timeDiff 的值，为下面的条件2服务
            if (timeDiff < maxTimeMillis) {
                end = System.currentTimeMillis();
                timeDiff = end - start;
            }
            // 是否继续执行 compact 主要由如下三个条件触发：
            // 1，meta.getUsage() >= threshold 看是否达到指定的阈值
            // 2， (maxTimeMillis > 0 && timeDiff > maxTimeMillis) 看是否达到预设的 GC compaction 最大执行的时间
            // 3， 是否为 running 的状态
            // 所以当 maxTimeMillis 设置为-1时，条件2永远无法为true，即只有达到预设的 GC 阈值才会退出 compactor 的操作
            if (meta.getUsage() >= threshold || (maxTimeMillis > 0 && timeDiff > maxTimeMillis) || !running) {
                // We allow the usage limit calculation to continue so that we get a accurate
                // report of where the usage was prior to running compaction.
                continue;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting entry log {} with usage {} below threshold {}",
                        meta.getEntryLogId(), meta.getUsage(), threshold);
            }

            long priorRemainingSize = meta.getRemainingSize();
            // 真正触发回收的核心逻辑
            compactEntryLog(meta);
            gcStats.getReclaimedSpaceViaCompaction().add(meta.getTotalSize() - priorRemainingSize);
            compactedBuckets[bucketIndex]++;
        }
        if (LOG.isDebugEnabled()) {
            if (!running) {
                LOG.debug("Compaction exited due to gc not running");
            }
            if (timeDiff > maxTimeMillis) {
                LOG.debug("Compaction ran for {}ms but was limited by {}ms", timeDiff, maxTimeMillis);
            }
        }
        // entryLogUsageBuckets：没压缩之前使用的是多少
        // compactedBuckets：压缩之后的值是多少
        // cost：该次压缩总共话费了多少时间（System.currentTimeMillis() - start）
        LOG.info(
                "Compaction: entry log usage buckets[10% 20% 30% 40% 50% 60% 70% 80% 90% 100%] = {}, compacted {}, cost"
                        + " {}ms",
                entryLogUsageBuckets, compactedBuckets, System.currentTimeMillis() - start);
    }

    /**
     * Calculate the index for the batch based on the usage between 0 and 1.
     *
     * @param numBuckets Number of reporting buckets.
     * @param usage 0.0 - 1.0 value representing the usage of the entry log.
     * @return index based on the number of buckets The last bucket will have the 1.0 if added.
     */
    int calculateUsageIndex(int numBuckets, double usage) {
        return Math.min(
                numBuckets - 1,
                (int) Math.floor(usage * numBuckets));
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    public void shutdown() throws InterruptedException {
        this.running = false;
        LOG.info("Shutting down GarbageCollectorThread");

        while (!compacting.compareAndSet(false, true)) {
            // Wait till the thread stops compacting
            Thread.sleep(100);
        }

        // Interrupt GC executor thread
        gcExecutor.shutdownNow();
    }

    /**
     * Remove entry log.
     *
     * 移除 EntryLog 分为两步：
     * 1，先去调用 OS File 去删除系统中的 EntryLog 文件
     * 2，再去删除本地 entryLogMetaMap 中对应的元素
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected void removeEntryLog(long entryLogId) {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            LOG.info("Removing entry log metadata for {}", entryLogId);
            // 如果删除成功，那么从本地的entryLogMetaMap中移除当前 EntryLog 的映射
            entryLogMetaMap.remove(entryLogId);
        }
    }

    /**
     * Compact an entry log.
     *
     * @param entryLogMeta
     */
    protected void compactEntryLog(EntryLogMetadata entryLogMeta) {
        // Similar with Sync Thread
        // try to mark compacting flag to make sure it would not be interrupted
        // by shutdown during compaction. otherwise it will receive
        // ClosedByInterruptException which may cause index file & entry logger
        // closed and corrupted.
        // 确保是同步操作
        if (!compacting.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates that compaction is in progress for this EntryLogId.
            return;
        }

        try {
            // Do the actual compaction
            compactor.compact(entryLogMeta);
        } catch (Exception e) {
            LOG.error("Failed to compact entry log {} due to unexpected error", entryLogMeta.getEntryLogId(), e);
        } finally {
            // Mark compaction done
            compacting.set(false);
        }
    }

    /**
     * Method to read in all of the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @param entryLogMetaMap
     *          Existing EntryLogs to Meta
     * @throws IOException
     */
    protected Map<Long, EntryLogMetadata> extractMetaFromEntryLogs(Map<Long, EntryLogMetadata> entryLogMetaMap) {
        // Extract it for every entry log except for the current one.
        // Entry Log ID's are just a long value that starts at 0 and increments
        // by 1 when the log fills up and we roll to a new one.
        long curLogId = entryLogger.getLeastUnflushedLogId();
        boolean hasExceptionWhenScan = false;
        for (long entryLogId = scannedLogId; entryLogId < curLogId; entryLogId++) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: {}", entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId);
                removeIfLedgerNotExists(entryLogMeta);
                if (entryLogMeta.isEmpty()) {
                    entryLogger.removeEntryLog(entryLogId);
                } else {
                    entryLogMetaMap.put(entryLogId, entryLogMeta);
                }
            } catch (IOException e) {
                hasExceptionWhenScan = true;
                LOG.warn("Premature exception when processing " + entryLogId
                         + " recovery will take care of the problem", e);
            }

            // if scan failed on some entry log, we don't move 'scannedLogId' to next id
            // if scan succeed, we don't need to scan it again during next gc run,
            // we move 'scannedLogId' to next id
            if (!hasExceptionWhenScan) {
                ++scannedLogId;
            }
        }
        return entryLogMetaMap;
    }

    CompactableLedgerStorage getLedgerStorage() {
        return ledgerStorage;
    }

    public GarbageCollectionStatus getGarbageCollectionStatus() {
        return GarbageCollectionStatus.builder()
            .forceCompacting(forceGarbageCollection.get())
            .majorCompacting(majorCompacting.get())
            .minorCompacting(minorCompacting.get())
            .lastMajorCompactionTime(lastMajorCompactionTime)
            .lastMinorCompactionTime(lastMinorCompactionTime)
            .majorCompactionCounter(gcStats.getMajorCompactionCounter().get())
            .minorCompactionCounter(gcStats.getMinorCompactionCounter().get())
            .build();
    }
}
