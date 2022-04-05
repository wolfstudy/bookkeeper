package org.apache.bookkeeper.stats.barad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

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
        assertEquals(baradMetric.getDimension(), counter.getLabels());
        assertEquals(baradMetric.getValue(), BigDecimal.valueOf(1));
        assertTrue(Maps.difference(baradMetric.getDimension(), labels).areEqual());
        assertTrue(System.currentTimeMillis() >= baradMetric.getTimestamp());
        
    }

    @Test
    public void testConvertSample() {

    }

    @Test
    public void testConvertGauge() {

    }

    @Test
    public void testConvertOpStat() {

    }
}
