package org.apache.bookkeeper.stats.barad.convert;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;


public class GaugeConverter implements IConverter<String, SimpleGauge<? extends Number>, BaradMetric> {

    private static final Set<String> names = new HashSet<>();

    @Override
    public BaradMetric convert(String name, SimpleGauge<? extends Number> source) {
        BaradMetric metric = new BaradMetric(name);
        metric.getDimension().putAll(source.getLabels());
        metric.setValue(BigDecimal.valueOf(source.getSample().doubleValue()));
        return metric;
    }

    @Override
    public boolean canConvert(String name) {
        return names.contains(name);
    }
}
