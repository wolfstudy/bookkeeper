package org.apache.bookkeeper.stats.barad.reporter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Monitor {

    private final ScheduledExecutorService executor;
    private final AtomicLong counter = new AtomicLong();

    private static final int PERIOD = 30;

    private static final Monitor instance;

    private final AtomicLong metricsCount = new AtomicLong(0L);

    public static Monitor getInstance() {
        return instance;
    }

    static {
        instance = new Monitor();
    }

    private Monitor() {
        executor = Executors.newScheduledThreadPool(1);
    }

    public void addMetricsCount(long count) {
        metricsCount.addAndGet(count);
    }


    public void resetTicker() {
        counter.set(System.currentTimeMillis());
    }

    public void start() {
        executor.scheduleAtFixedRate(() -> {
            long last = counter.get();
            long duration = System.currentTimeMillis() - last;
            if (duration > PERIOD * 1000) {
                log.warn("No input message since {}", last);
            }
            long metrics = metricsCount.getAndSet(0L);
            
            log.warn("metrics uploaded {}", metrics);
        }, 1, PERIOD, TimeUnit.SECONDS);
    }

    public void stop() {
        executor.shutdown();
    }
}
