package org.apache.bookkeeper.stats.barad.convert;

import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;

public class SampleConverter implements IConverter<String, Sample, BaradMetric> {

    private static final Set<String> names = new HashSet<>();

    static {
        names.add("jvm_memory_bytes_committed");
        names.add("jvm_memory_bytes_init");
        names.add("jvm_memory_bytes_max");
        names.add("jvm_memory_direct_bytes_max");
        names.add("jvm_heap_used");
        names.add("jvm_max_memory");

        names.add("jvm_max_direct_memory");
        names.add("jvm_direct_memory_used");
        names.add("jvm_memory_bytes_used");
        names.add("jvm_memory_direct_bytes_used");

        names.add("jvm_classes_loaded");
        names.add("jvm_threads_current");
        names.add("jvm_threads_daemon");
        names.add("jvm_threads_deadlocked");
        names.add("jvm_threads_peak");

        names.add("jvm_gc_collection_seconds_count");
        names.add("jvm_gc_collection_seconds_sum");

        //add more metric for process and log4j2
        names.add("process_open_fds");
        names.add("process_max_fds");
        names.add("process_virtual_memory_bytes");
        names.add("process_resident_memory_bytes");
        names.add("log4j2_appender_total");

    }


    @Override
    public BaradMetric convert(String name, Sample sample) {
        BaradMetric baradMetric = new BaradMetric(name);
        baradMetric.setValue(BigDecimal.valueOf(sample.value));
        baradMetric.getDimension().put("bkip",bkip);
        baradMetric.getDimension().put("ip",bkip);

        if (sample.labelNames != null && sample.labelValues != null) {
            int bound = Math.min(sample.labelNames.size(), sample.labelValues.size());
            for (int i = 0; i < bound; i++) {
                baradMetric.getDimension().put(sample.labelNames.get(i), sample.labelValues.get(i));
            }
        }
        return baradMetric;
    }

    @Override
    public boolean canConvert(String name) {
        return names.contains(name);
    }
}
