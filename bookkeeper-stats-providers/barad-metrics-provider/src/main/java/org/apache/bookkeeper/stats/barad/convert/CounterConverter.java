package org.apache.bookkeeper.stats.barad.convert;

import com.google.common.base.Strings;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.barad.reporter.NetUtil;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;

public class CounterConverter implements IConverter<String, LongAdderCounter, BaradMetric> {

    private static final Set<String> names = new HashSet<>();

    static {
        names.add("bookie_journal_JOURNAL_CB_QUEUE_SIZE");
        names.add("bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE");
        names.add("bookie_journal_JOURNAL_QUEUE_SIZE");
        names.add("bookie_entries_count");
        names.add("bookie_deleted_ledger_count");
        names.add("bookie_ledgers_count");
        names.add("bookie_read_cache_size");
        names.add("bookie_SERVER_STATUS");
        names.add("bookie_write_cache_size");

        names.add("bookie_gc_MINOR_COMPACTION_COUNT");
        names.add("bookie_gc_MAJOR_COMPACTION_COUNT");

        names.add("bookie_LEDGER_CACHE_HIT");
        names.add("bookie_LEDGER_CACHE_MISS");
        names.add("bookie_LEDGER_CACHE_NUM_EVICTED_LEDGERS");
    }

    @Override
    public BaradMetric convert(String name, LongAdderCounter source) {
        BaradMetric metric = new BaradMetric(name);
        metric.getDimension().putAll(source.getLabels());
        metric.getDimension().put("bkip",bkip);
        metric.getDimension().put("ip",bkip);
        metric.setValue(BigDecimal.valueOf(source.get()));
        return metric;
    }

    @Override
    public boolean canConvert(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return false;
        }
        return names.contains(name);
    }
}
