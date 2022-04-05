package org.apache.bookkeeper.stats.barad.reporter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Monitor {

    private final ScheduledExecutorService executor;
    private final AtomicLong totalCount = new AtomicLong();

    private static final int PERIOD = 5;

    public Monitor() {
        executor = Executors.newScheduledThreadPool(1);
    }


    public void addTotalCount(int count) {
        totalCount.addAndGet(count);
    }

    public void start() {
        executor.scheduleAtFixedRate(() -> {
            long count = totalCount.get();
            long countPerSecond = count / PERIOD;
            log.info("Monitor counter, total = {}, average = {}/s", count, countPerSecond);
            totalCount.set(0);
        }, 1, PERIOD, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdown();
    }

}
