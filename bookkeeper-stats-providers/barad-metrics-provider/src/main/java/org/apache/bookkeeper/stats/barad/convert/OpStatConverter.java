package org.apache.bookkeeper.stats.barad.convert;

import com.google.common.base.Strings;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.DataSketchesOpStatsLogger;

public class OpStatConverter implements IConverter<String, DataSketchesOpStatsLogger, BaradMetric> {

    // Example:
    // # TYPE bookie_journal_JOURNAL_ADD_ENTRY summary
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.5",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.75",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.95",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.99",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.999",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="0.9999",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY{success="false",quantile="1.0",} NaN
    // bookie_journal_JOURNAL_ADD_ENTRY_count{success="false",} 0.0
    // bookie_journal_JOURNAL_ADD_ENTRY_sum{success="false",} 0.0
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.5",} 1.706
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.75",} 1.89
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.95",} 2.121
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.99",} 10.708
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.999",} 10.902
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="0.9999",} 10.902
    // bookie_journal_JOURNAL_ADD_ENTRY{success="true",quantile="1.0",} 10.902
    // bookie_journal_JOURNAL_ADD_ENTRY_count{success="true",} 658.0
    // bookie_journal_JOURNAL_ADD_ENTRY_sum{success="true",} 1265.0800000000002

    private static final Set<String> names = new HashSet<>();

    static {
        names.add("bookie_journal_JOURNAL_SYNC_count");
        names.add("bookie_journal_JOURNAL_ADD_ENTRY");
        names.add("bookie_journal_JOURNAL_SYNC");
        names.add("bookkeeper_server_ADD_ENTRY_REQUEST");
        names.add("bookkeeper_server_READ_ENTRY_REQUEST");
    }

    @Override
    public BaradMetric convert(String name, DataSketchesOpStatsLogger source) {
        throw new UnsupportedOperationException("OpStatsLogger only support multi metrics");
    }

    @Override
    public List<BaradMetric> multiConvert(String name, DataSketchesOpStatsLogger opStat) {

        List<BaradMetric> baradMetrics = new ArrayList<>(18);

        baradMetrics.add(convertSum(opStat, name, true));
        baradMetrics.add(convertCount(opStat, name, true));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.5));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.75));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.95));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.99));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.999));
        baradMetrics.add(convertQuantile(opStat, name, true, 0.9999));
        baradMetrics.add(convertQuantile(opStat, name, true, 1.0));

        baradMetrics.add(convertSum(opStat, name, false));
        baradMetrics.add(convertCount(opStat, name, false));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.5));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.75));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.95));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.99));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.999));
        baradMetrics.add(convertQuantile(opStat, name, false, 0.9999));
        baradMetrics.add(convertQuantile(opStat, name, false, 1.0));
        
        return baradMetrics;
    }

    private BaradMetric convertQuantile(DataSketchesOpStatsLogger opStat, String name, Boolean success,
            double quantile) {
        String newName=name.toLowerCase() + "_" + (success ? "success" : "failure") +
                "_" + formatQuantile(String.valueOf(quantile));
        BaradMetric baradMetric = new BaradMetric(newName);
        baradMetric.getDimension().putAll(opStat.getLabels());
        
        baradMetric.setValue(BigDecimal.valueOf(opStat.getQuantileValue(success,quantile)));
        return baradMetric;
    }

    private BaradMetric convertSum(DataSketchesOpStatsLogger opStat, String name, Boolean success) {
        String newName=name.toLowerCase() + "_sum_"+ (success ? "success" : "failure");
        BaradMetric baradMetric = new BaradMetric(newName);
        baradMetric.getDimension().putAll(opStat.getLabels());

        baradMetric.setValue(BigDecimal.valueOf(opStat.getSum(success)));
        return baradMetric;
    }

    private BaradMetric convertCount(DataSketchesOpStatsLogger opStat, String name, Boolean success) {
        String newName=name.toLowerCase() + "_count_"+ (success ? "success" : "failure");
        BaradMetric baradMetric = new BaradMetric(newName);
        baradMetric.getDimension().putAll(opStat.getLabels());
        baradMetric.setValue(BigDecimal.valueOf(opStat.getCount(success)));
        return baradMetric;
    }

    @Override
    public boolean canConvert(String name) {
        return names.contains(name);
    }

    static String formatQuantile(String quantile) {
        if (Strings.isNullOrEmpty(quantile)) {
            return null;
        } else {
            return quantile.replace("\\.", "_");
        }
    }
}
