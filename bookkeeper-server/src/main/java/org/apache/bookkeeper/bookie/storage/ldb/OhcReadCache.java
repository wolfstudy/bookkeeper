package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.OHCacheStats;

@Slf4j
public class OhcReadCache implements ReadCache {

    private final static ScheduledExecutorService statsExecutor = Executors
            .newSingleThreadScheduledExecutor(new DefaultThreadFactory("read-cache-stats"));
    private static OHCache<LongPairWrapper, ByteBuf> cache = null;
    private static OhcReadCacheStats ohcReadCacheStats;
    private static volatile OHCacheStats oldStats;


    public OhcReadCache(ByteBufAllocator allocator, long maxCacheSize) {
        if(this.cache ==null) {
            this.cache = OHCacheBuilder.<LongPairWrapper, ByteBuf>newBuilder()
                    .keySerializer(new LongPairSerializer())
                    .valueSerializer(new ByteBufSerializer())
                    .eviction(Eviction.LRU)
                    .capacity(maxCacheSize)
                    .hashTableSize(16 * 1024)
                    .build();
            this.ohcReadCacheStats = new OhcReadCacheStats("ReadCache");
            this.statsExecutor.scheduleWithFixedDelay(this::printStatsLog, 1, 1, TimeUnit.MINUTES);
            log.info("ReadCache init maxCacheSize {} allocator: {} ", maxCacheSize, allocator);
        }
    }

    @Override
    public void put(long ledgerId, long entryId, ByteBuf entry) {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        try {
            cache.put(key, entry);
        } finally {
            key.recycle();
        }
    }

    @Override
    public ByteBuf get(long ledgerId, long entryId) {

        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        try {
            ByteBuf value = cache.get(key);
            if (value == null) {
                this.ohcReadCacheStats.incMiss();
            } else {
                this.ohcReadCacheStats.incrHit();
            }
            return value;
        } finally {
            key.recycle();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            cache.close();
        } catch (Exception e) {
            log.error("close ohc cache failed", e);
        }
    }

    @Override
    public long size() {
        return cache.memUsed();
    }

    @Override
    public long count() {
        return cache.size();
    }

    /**
     * only for unit test use
     */
    public void clear() {
        try {
            cache.clear();
        } catch (Exception e) {
            log.error("clear ohc cache failed", e);
        }
    }

    public String printStatsLog() {
        String ret = "";
        if (this.cache != null) {
            OHCacheStats newStats = this.cache.stats();
            if (newStats != null) {
                this.ohcReadCacheStats.setOHCacheStats(newStats, oldStats);
                log.info(this.ohcReadCacheStats.toString());
                ret = this.ohcReadCacheStats.printStatsLog();
                this.oldStats = newStats;
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return this.ohcReadCacheStats.toString();
    }
}
