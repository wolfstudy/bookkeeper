package org.apache.bookkeeper.stats.barad.convert;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;


public class GaugeConverter implements IConverter<String, SimpleGauge<? extends Number>, BaradMetric> {

    private static final Set<String> names = new HashSet<>();

    static {
        names.add("unit_test_gauge");
        names.add("bookkeeper_server_READ_ENTRY_IN_PROGRESS");
        names.add("bookkeeper_server_ADD_ENTRY_IN_PROGRESS");
        names.add("bookie_ledger_writable_dirs");
        names.add("bookkeeper_server_ADD_ENTRY_BLOCKED");

        names.add("bookie_journal_JOURNAL_MEMORY_USED");
        names.add("bookie_NUM_INDEX_PAGES");


        names.add("bookie_SERVER_STATUS");
        names.add("bookie_gc_ACTIVE_ENTRY_LOG_SPACE_BYTES");
        names.add("bookkeeper_server_READ_ENTRY_BLOCKED");
        names.add("bookie_gc_ACTIVE_LEDGER_COUNT");
        names.add("bookie_gc_ACTIVE_ENTRY_LOG_COUNT");
    }

    @Override
    public BaradMetric convert(String name, SimpleGauge<? extends Number> source) {
        BaradMetric metric = new BaradMetric(name);
        metric.getDimension().putAll(source.getLabels());
        metric.getDimension().put("bkip", bkip);
        metric.getDimension().put("ip", bkip);
        if (source.getSample() != null) {
            metric.setValue(BigDecimal.valueOf(source.getSample().doubleValue()));
        } else {
            return null;
        }
        return metric;
    }

    @Override
    public boolean canConvert(String name) {
        return names.contains(name);
    }
}
