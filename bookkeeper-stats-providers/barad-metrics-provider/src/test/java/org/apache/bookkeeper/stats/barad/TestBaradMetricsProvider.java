package org.apache.bookkeeper.stats.barad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.math.BigDecimalMath;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.ClassLoadingExports;
import io.prometheus.client.hotspot.ThreadExports;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.convert.GaugeConverter;
import org.apache.bookkeeper.stats.barad.convert.OpStatConverter;
import org.apache.bookkeeper.stats.barad.convert.SampleConverter;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.DataSketchesOpStatsLogger;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.Test;


@Slf4j
public class TestBaradMetricsProvider {

    @Test
    public void testStartNoHttp() {
        PropertiesConfiguration config = new PropertiesConfiguration();
        config.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        BaradMetricsProvider provider = new BaradMetricsProvider();
        try {
            provider.start(config);
        } finally {
            provider.stop();
        }
    }

    @Test
    public void testConvertCounter() {

        String name = "bookie_ledgers_count";
        Map<String, String> labels = new HashMap<>();
        labels.put("labelK1", "labelV1");
        labels.put("labelK2", "labelV2");
        LongAdderCounter counter = new LongAdderCounter(labels);
        counter.add(1);

        CounterConverter counterConverter = new CounterConverter();
        assertTrue(counterConverter.canConvert("bookie_ledgers_count"));
        BaradMetric baradMetric = counterConverter.convert(name, counter);

        assertEquals(baradMetric.getName(), name);
        assertEquals(baradMetric.getValue(), BigDecimal.valueOf(1));
        assertTrue(baradMetric.getDimension().keySet().containsAll(labels.keySet()));
        assertTrue(System.currentTimeMillis() >= baradMetric.getTimestamp());

    }

    @Test
    public void testConvertSample() {

        SampleConverter sampleConverter = new SampleConverter();
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        registry.register(new ThreadExports());
        registry.register(new ClassLoadingExports());

        Enumeration<MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();
        List<BaradMetric> metrics = new ArrayList<>();
        while (metricFamilySamples.hasMoreElements()) {
            MetricFamilySamples metricFamily = metricFamilySamples.nextElement();
            for (Sample sample : metricFamily.samples) {
                if (sampleConverter.canConvert(sample.name)) {
                    BaradMetric baradMetric = sampleConverter.convert(sample.name, sample);
                    System.out.println(baradMetric.toString());
                    metrics.add(baradMetric);
                } else {
                    log.warn("ignore sample [{}]", sample.name);
                }
            }
        }

        assertEquals(metrics.size(), 5);
    }

    @Test
    public void testConvertGauge() {

        String name = "unit_test_gauge";
        Map<String, String> labels = new HashMap<>();
        labels.put("labelK1", "labelV1");
        labels.put("labelK2", "labelV2");

        SimpleGauge<Long> simpleGauge = new SimpleGauge<Long>(new MockGauge(), labels);

        GaugeConverter gaugeConverter = new GaugeConverter();
        assertTrue(gaugeConverter.canConvert(name));
        BaradMetric baradMetric = gaugeConverter.convert(name, simpleGauge);

        assertEquals(baradMetric.getName(), name);

        assertEquals(baradMetric.getValue(), BigDecimal.valueOf(1.0));
        assertTrue(baradMetric.getDimension().keySet().containsAll(labels.keySet()));
        assertTrue(System.currentTimeMillis() >= baradMetric.getTimestamp());

    }

    @Test
    public void testConvertOpStat() {
        String name = "bookie_journal_JOURNAL_SYNC";
        Map<String, String> labels = new HashMap<>();
        labels.put("labelK1", "labelV1");
        labels.put("labelK2", "labelV2");

        DataSketchesOpStatsLogger dataSketchesOpStatsLogger = new DataSketchesOpStatsLogger(labels);

        dataSketchesOpStatsLogger.registerSuccessfulEvent(100, TimeUnit.MILLISECONDS);
        dataSketchesOpStatsLogger.registerSuccessfulEvent(50, TimeUnit.MILLISECONDS);

        dataSketchesOpStatsLogger.registerFailedEvent(200, TimeUnit.MILLISECONDS);
        dataSketchesOpStatsLogger.registerFailedEvent(500, TimeUnit.MILLISECONDS);
        dataSketchesOpStatsLogger.registerFailedEvent(300, TimeUnit.MILLISECONDS);

        dataSketchesOpStatsLogger.rotateLatencyCollection();

        OpStatConverter opStatConverter = new OpStatConverter();

        assertTrue(opStatConverter.canConvert(name));

        List<BaradMetric> baradMetrics = opStatConverter.multiConvert(name, dataSketchesOpStatsLogger);
        for (BaradMetric metric : baradMetrics) {
            System.out.println(metric.toString());
        }

        assertEquals(baradMetrics.size(), 18);


    }

    public static class MockGauge implements Gauge<Long> {

        @Override
        public Long getDefaultValue() {
            return 0L;
        }

        @Override
        public Long getSample() {
            return 1L;
        }
    }
}
