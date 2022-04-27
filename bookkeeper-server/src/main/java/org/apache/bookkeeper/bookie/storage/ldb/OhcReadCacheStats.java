package org.apache.bookkeeper.bookie.storage.ldb;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.CATEGORY_SERVER;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.caffinitas.ohc.OHCacheStats;

@StatsDoc(
        name = BOOKIE_SCOPE,
        category = CATEGORY_SERVER,
        help = "OHC read cache stats"
)
@Getter
@Slf4j
public class OhcReadCacheStats {

    private OHCacheStats oldStats;
    private OHCacheStats newStats;

    private int hitCounter;
    private int missCounter;
    private String name;


    public OhcReadCacheStats(String name) {
        this.name = name;
        this.hitCounter = 0;
        this.missCounter = 0;
    }

    public void setOHCacheStats(OHCacheStats newStats, OHCacheStats oldStats) {
        this.newStats = newStats;
        this.oldStats = oldStats;
    }

    public synchronized void incrHit() {
        this.hitCounter++;
    }

    public synchronized void incMiss() {
        this.missCounter++;
    }

    private static long maxOf(long[] arr) {
        long r = 0;
        for (long l : arr) {
            if (l > r) {
                r = l;
            }
        }
        return r;
    }

    private static long minOf(long[] arr) {
        long r = Long.MAX_VALUE;
        for (long l : arr) {
            if (l < r) {
                r = l;
            }
        }
        return r;
    }

    private static double avgOf(long[] arr) {
        double r = 0d;
        for (long l : arr) {
            r += l;
        }
        return r / arr.length;
    }

    public double getAverageSegmentSize() {
        return avgOf(newStats.getSegmentSizes());
    }

    public long getMinSegmentSize() {
        return minOf(newStats.getSegmentSizes());
    }

    public long getMaxSegmentSize() {
        return maxOf(newStats.getSegmentSizes());
    }

    public StringBuilder add(StringBuilder sb, String key, Object v) {
        sb.append("  ").append(key).append(" = ").append(v).append("\n");
        return sb;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        add(sb, "readHitRatio", getReadBlockHitRatio());
        add(sb, "OHCHitRatio", getOHCHitRatio(this.newStats));
        add(sb, "hitCount", newStats.getHitCount());
        add(sb, "missCount", newStats.getMissCount());
        add(sb, "evictionCount", newStats.getEvictionCount());
        add(sb, "expireCount", newStats.getExpireCount());
        add(sb, "size", newStats.getSize());
        add(sb, "capacity", newStats.getCapacity());
        add(sb, "free", newStats.getFree());
        add(sb, "rehashCount", newStats.getRehashCount());
        add(sb, "put(add/replace/fail)", Long.toString(newStats.getPutAddCount()) + '/' + newStats.getPutReplaceCount()
                + '/' + newStats.getPutFailCount());
        add(sb, "removeCount", newStats.getRemoveCount());
        add(sb, "segmentSizes(#/min/max/avg)", String.format("%d/%d/%d/%.2f", newStats.getSegmentSizes().length,
                getMinSegmentSize(), getMaxSegmentSize(), getAverageSegmentSize()));
        add(sb, "totalAllocated", newStats.getTotalAllocated());
        add(sb, "lruCompactions", newStats.getLruCompactions());
        return sb.toString();
    }

    public long getTotalPut(OHCacheStats stats) {
        if (stats == null) {
            return 0;
        }
        return stats.getPutAddCount() + stats.getPutReplaceCount() + stats.getPutFailCount();
    }

    public long getTotalGet(OHCacheStats stats) {
        if (stats == null) {
            return 0;
        }
        return stats.getHitCount() + stats.getMissCount();
    }

    public String printStatsLog() {
        long gets = getTotalGet(this.newStats) - getTotalGet(oldStats);
        long puts = getTotalPut(this.newStats) - getTotalPut(oldStats);
        long removes = this.newStats.getRemoveCount() - (oldStats != null ? oldStats.getRemoveCount() : 0L);
        long evictions = this.newStats.getEvictionCount() - (oldStats != null ? oldStats.getEvictionCount() : 0L);
        long used = newStats.getCapacity() - newStats.getFree();

        String msg = String.format("%s, [%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s]", this.name, getReadBlockHitRatioAndReset(),
                getOHCHitRatio(newStats), gets, puts, removes, evictions, newStats.getSize(), used, newStats.getFree(),
                newStats.getCapacity(), getMemoryUsedRatio(newStats));
        log.info(msg);
        return msg;
    }

    public synchronized void reset() {
        this.hitCounter = 0;
        this.missCounter = 0;
    }

    public synchronized int getReadBlockHitRatioAndReset() {
        int ret = this.getReadBlockHitRatio();
        this.reset();
        return ret;
    }

    public synchronized int getReadBlockHitRatio() {
        int total = this.hitCounter + this.missCounter;
        return total > 0 ? (int) Math.round((double) this.hitCounter / total * 100) : 0;
    }

    public int getOHCHitRatio(OHCacheStats stats) {
        long totalGet = getTotalGet(stats);
        return totalGet > 0 ? (int) Math.round((double) stats.getHitCount() / totalGet * 100) : 0;
    }

    public int getMemoryUsedRatio(OHCacheStats stats) {
        int usedRation = stats.getCapacity() > 0 ? Math.round((stats.getCapacity() - stats.getFree()) * 100.0F
                / stats.getCapacity()) : 0;
        return usedRation;
    }
}
