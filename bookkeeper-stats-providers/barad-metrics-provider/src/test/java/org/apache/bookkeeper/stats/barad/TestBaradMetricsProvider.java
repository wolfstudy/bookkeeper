package org.apache.bookkeeper.stats.barad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.math.BigDecimalMath;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.convert.GaugeConverter;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;
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
        assertEquals(baradMetric.getValue(), BigDecimal.valueOf(1));
        assertTrue(baradMetric.getDimension().keySet().containsAll(labels.keySet()));
        assertTrue(System.currentTimeMillis() >= baradMetric.getTimestamp());
        
    }

    @Test
    public void testConvertSample() {
        List<Integer> list =new ArrayList<Integer>();
        list.add(null);
        System.out.println(list.size());
        assertEquals(list.size(),1);
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
