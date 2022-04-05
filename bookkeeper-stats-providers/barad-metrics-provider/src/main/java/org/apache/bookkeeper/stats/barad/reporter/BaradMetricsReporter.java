package org.apache.bookkeeper.stats.barad.reporter;


import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.convert.GaugeConverter;
import org.apache.bookkeeper.stats.barad.convert.OpStatConverter;
import org.apache.bookkeeper.stats.barad.convert.SampleConverter;
import org.apache.bookkeeper.stats.prometheus.DataSketchesOpStatsLogger;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;

@Slf4j
public class BaradMetricsReporter implements Reporter<BaradMetric> {

    private final JobQueue jobQueue = new JobQueue();

    private final ExecutorService executorService;

    private ReportWorker worker;

    private final MetricsReporterConfig config;

    private final Monitor monitor;

    private CounterConverter counterConverter;
    private GaugeConverter gaugeConverter;
    private OpStatConverter opStatConverter;
    private SampleConverter sampleConverter;

    public BaradMetricsReporter(MetricsReporterConfig config) {
        this.config = config;
        executorService = Executors.newFixedThreadPool(1);
        counterConverter = new CounterConverter();
        gaugeConverter = new GaugeConverter();
        opStatConverter = new OpStatConverter();
        sampleConverter = new SampleConverter();
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
    public synchronized void report(List<BaradMetric> metrics) {
        if (!worker.isRunning()) {
            log.warn("Worker is dead, start a new one.");
            startWorker();
        }
        jobQueue.add(JobQueue.Job.builder().metrics(metrics).build());
    }

    @Override
    public void shutdown() {
        worker.terminate();
        executorService.shutdown();
        monitor.stop();
    }

    public void reportRegistry(CollectorRegistry registry) {
        Enumeration<MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();
        while (metricFamilySamples.hasMoreElements()) {
            MetricFamilySamples metricFamily = metricFamilySamples.nextElement();
            for (Sample sample : metricFamily.samples) {
                reportSample(sample);
            }
        }
    }

    public void reportSample(Sample sample) {
        if (sampleConverter.canConvert(sample.name)) {
            BaradMetric baradMetric = sampleConverter.convert(sample.name, sample);
            report(Arrays.asList(baradMetric));
            log.info("report {}", baradMetric);

        } else {
            log.warn("ignore [{}]", sample.name);
        }
    }

    public void reportGauge(String name, SimpleGauge<? extends Number> gauge) {
        if (gaugeConverter.canConvert(name)) {
            BaradMetric baradMetric = gaugeConverter.convert(name, gauge);
            report(Arrays.asList(baradMetric));
            log.info("report {}", baradMetric);
        } else {
            log.warn("ignore [{}]", name);
        }
    }

    public void reportCounter(String name, LongAdderCounter counter) {
        if (counterConverter.canConvert(name)) {
            BaradMetric baradMetric = counterConverter.convert(name, counter);
            report(Arrays.asList(baradMetric));
            log.info("report {}", baradMetric);
        } else {
            log.warn("ignore [{}]", name);
        }
    }

    public void reportOpStat(String name, DataSketchesOpStatsLogger opStat) {
        if (opStatConverter.canConvert(name)) {
            List<BaradMetric> baradMetrics = opStatConverter.multiConvert(name, opStat);
            report(baradMetrics);
            for (BaradMetric metric : baradMetrics) {
                log.info("report {}", metric);
            }
        } else {
            log.warn("ignore [{}]", name);
        }
    }
}