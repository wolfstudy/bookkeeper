package org.apache.bookkeeper.stats.barad.reporter;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaradMetricsReporter implements Reporter<BaradMetric> {

    private final JobQueue jobQueue = new JobQueue();

    private final ExecutorService executorService;

    private ReportWorker worker;

    private final MetricsReporterConfig config;

    private final Monitor monitor;

    public BaradMetricsReporter(MetricsReporterConfig config) {
        this.config = config;
        executorService = Executors.newFixedThreadPool(1);
        monitor = new Monitor();
        monitor.start();
        startWorker();
    }

    private void startWorker() {
        HttpClient client = new HttpClient(config.getUrl(),
                config.getConnectionTimeoutMs(), config.getConnectionTimeoutMs(), config.getConnectionNum());
        client.addMonitor(monitor);
        worker = new ReportWorker(client, config.getBatchSizeLimit(), jobQueue);
        executorService.submit(worker);
    }


    @Override
    public synchronized void report(List<BaradMetric> metrics, CompletableFuture<?> future) {
        if (!worker.isRunning()) {
            log.warn("Worker is dead, start a new one.");
            startWorker();
        }
        jobQueue.add(JobQueue.Job.builder().metrics(metrics).notifyFuture(future).build());
    }

    @Override
    public void shutdown() {
        worker.terminate();
        executorService.shutdown();
        monitor.stop();
    }
}