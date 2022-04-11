package org.apache.bookkeeper.stats.barad.reporter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.convert.GaugeConverter;
import org.apache.bookkeeper.stats.barad.convert.OpStatConverter;
import org.apache.bookkeeper.stats.barad.convert.SampleConverter;
import org.apache.bookkeeper.stats.prometheus.DataSketchesOpStatsLogger;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.ScopeContext;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;

@Slf4j
public class BaradMetricsReporter implements Reporter<BaradMetric> {


    private final ExecutorService dispatcher;
    private final MetricsReporterConfig config;
    private final BaradHttpClient httpClient;
    private final CounterConverter counterConverter;
    private final GaugeConverter gaugeConverter;
    private final OpStatConverter opStatConverter;
    private final SampleConverter sampleConverter;
    private final ObjectMapper mapper = new ObjectMapper();


    public BaradMetricsReporter(MetricsReporterConfig config) {
        this.config = config;
        this.counterConverter = new CounterConverter();
        this.gaugeConverter = new GaugeConverter();
        this.opStatConverter = new OpStatConverter();
        this.sampleConverter = new SampleConverter();
        this.dispatcher = new ThreadPoolExecutor(1, config.getConnectionNum(),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(this.config.getQueueSize()));
        this.httpClient = new BaradHttpClient(this.config.getBaradUrl()
                , this.config.getConnectionTimeoutMs(), this.config.getConnectionTimeoutMs());
    }


    @Override
    public synchronized void report(List<BaradMetric> metrics) {
        try {
            addMetricsToQueue(metrics);
            Monitor.getInstance().resetTicker();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    private void addMetricsToQueue(List<BaradMetric> metrics) throws InterruptedException {
        int batchIndex = 0;
        int batchCount = 1000;
        log.debug("Send metrics in {} batches,", metrics.size() / batchCount);
        while (batchIndex < metrics.size()) {
            int endIndex = batchIndex + batchCount;
            if (endIndex > metrics.size()) {
                endIndex = metrics.size();
            }
            List<BaradMetric> uploadMetrics = metrics.subList(batchIndex, endIndex);
            boolean isSubmitted = false;
            int haveTried = 0;
            while (!isSubmitted && haveTried < 3) {
                try {
                    dispatcher.submit(() -> {
                        try {
                            byte[] requestBody = mapper.writeValueAsBytes(uploadMetrics);
                            Monitor.getInstance().addMetricsCount(uploadMetrics.size());
                            okhttp3.Response response = httpClient.doPost(requestBody);
                            if (response.isSuccessful()) {
                                Response data = mapper.readValue(response.body().string(),
                                        Response.class);
                                if (data.getReturnValue() != 0) {
                                    log.error("Barad error response {}", data);
                                }
                            }
                        } catch (Exception e) {
                            log.error("exception running reporting", e);
                        }
                    });
                    isSubmitted = true;
                } catch (RejectedExecutionException e) {
                    haveTried++;
                    Thread.sleep(haveTried * 1000L);
                }
            }
            if (!isSubmitted) {
                log.error("tried times to submit, but failed.");
            }

            batchIndex += batchCount;
        }
    }

    @Override
    public void shutdown() {
        this.dispatcher.shutdown();
    }

    public void reportRegistry(CollectorRegistry registry) {
        Enumeration<MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();
        List<BaradMetric> metrics = new ArrayList<>();
        while (metricFamilySamples.hasMoreElements()) {
            MetricFamilySamples metricFamily = metricFamilySamples.nextElement();
            for (Sample sample : metricFamily.samples) {
                if (sampleConverter.canConvert(sample.name)) {
                    BaradMetric baradMetric = sampleConverter.convert(sample.name, sample);
                    log.info("report {}", baradMetric);
                    metrics.add(baradMetric);
                } else {
                    log.warn("ignore sample [{}]", sample.name);
                }
            }
        }
        report(metrics);
    }

    public void reportGaugeMap(ConcurrentMap<ScopeContext, SimpleGauge<? extends Number>> gauges) {

        List<BaradMetric> metrics = new ArrayList<>();
        for (Entry<ScopeContext, SimpleGauge<? extends Number>> entry : gauges.entrySet()) {
            String name = entry.getKey().getScope();
            if (gaugeConverter.canConvert(name)) {
                BaradMetric baradMetric = gaugeConverter.convert(name, entry.getValue());
                if(baradMetric!=null) {
                    log.info("report {}", baradMetric);
                    metrics.add(baradMetric);
                }
            } else {
                log.warn("ignore gauge [{}]", name);
            }
        }
        report(metrics);
    }

    public void reportCounterMap(ConcurrentMap<ScopeContext, LongAdderCounter> counters) {
        List<BaradMetric> metrics = new ArrayList<>();
        for (Entry<ScopeContext, LongAdderCounter> entry : counters.entrySet()) {
            String name = entry.getKey().getScope();
            if (counterConverter.canConvert(name)) {
                BaradMetric baradMetric = counterConverter.convert(name, entry.getValue());
                log.info("report {}", baradMetric);
                metrics.add(baradMetric);
            } else {
                log.warn("ignore counter [{}]", name);
            }
        }
        report(metrics);
    }

    public void reportOpStat(String name, DataSketchesOpStatsLogger opStat) {
        if (opStatConverter.canConvert(name)) {
            List<BaradMetric> baradMetrics = opStatConverter.multiConvert(name, opStat);
            
            report(baradMetrics);
            for (BaradMetric metric : baradMetrics) {
                log.info("report {}", metric);
            }
        } else {
            log.warn("ignore opstat [{}]", name);
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Response {
        private Integer returnValue;
        private Integer returnCode;
        private String msg;
        private BigDecimal seq;
    }
}