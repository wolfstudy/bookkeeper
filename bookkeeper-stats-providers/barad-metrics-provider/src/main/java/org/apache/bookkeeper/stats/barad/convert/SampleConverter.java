package org.apache.bookkeeper.stats.barad.convert;

import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;

public class SampleConverter implements IConverter<String, Sample, BaradMetric>{

    private static final Set<String> names = new HashSet<>();
    
    /* example
    * jvm_memory_bytes_used{area="heap"} 2.0604242528E10
    * jvm_memory_bytes_used{area="nonheap"} 8.097236E7
    //jvm_memory_bytes_committed{area="heap"} 3.4359738368E10
    //jvm_memory_bytes_committed{area="nonheap"} 8.5065728E7
    //jvm_memory_bytes_max{area="heap"} 3.4359738368E10
    //jvm_memory_bytes_max{area="nonheap"} -1.0
    //jvm_memory_bytes_init{area="heap"} 3.4359738368E10
    //jvm_memory_bytes_init{area="nonheap"} 2555904.0
    //jvm_memory_pool_bytes_used{pool="Code Cache"} 3.4739264E7
    //jvm_memory_pool_bytes_used{pool="Metaspace"} 4.6233096E7
    //jvm_memory_pool_bytes_used{pool="G1 Eden Space"} 9.2274688E8
    //jvm_memory_pool_bytes_used{pool="G1 Survivor Space"} 5.0331648E7
    //jvm_memory_pool_bytes_used{pool="G1 Old Gen"} 1.9631164E10
    //jvm_memory_pool_bytes_committed{pool="Code Cache"} 3.5520512E7
    //jvm_memory_pool_bytes_committed{pool="Metaspace"} 4.9545216E7
    //jvm_memory_pool_bytes_committed{pool="G1 Eden Space"} 1.76160768E9
    //jvm_memory_pool_bytes_committed{pool="G1 Survivor Space"} 5.0331648E7
    //jvm_memory_pool_bytes_committed{pool="G1 Old Gen"} 3.254779904E10
    //jvm_memory_pool_bytes_max{pool="Code Cache"} 2.5165824E8
    //jvm_memory_pool_bytes_max{pool="Metaspace"} -1.0
    //jvm_memory_pool_bytes_max{pool="G1 Eden Space"} -1.0
    //jvm_memory_pool_bytes_max{pool="G1 Survivor Space"} -1.0
    //jvm_memory_pool_bytes_max{pool="G1 Old Gen"} 3.4359738368E10
    //jvm_memory_pool_bytes_init{pool="Code Cache"} 2555904.0
    //jvm_memory_pool_bytes_init{pool="Metaspace"} 0.0
    //jvm_memory_pool_bytes_init{pool="G1 Eden Space"} 1.811939328E9
    //jvm_memory_pool_bytes_init{pool="G1 Survivor Space"} 0.0
    //jvm_memory_pool_bytes_init{pool="G1 Old Gen"} 3.254779904E10
    *log4j2_appender_total{level="debug"} 0.0
    *log4j2_appender_total{level="warn"} 108635.0
    *log4j2_appender_total{level="trace"} 0.0
    *log4j2_appender_total{level="error"} 1.3358339E7
    *log4j2_appender_total{level="fatal"} 0.0
    *log4j2_appender_total{level="info"} 5.9101963E7
    *jvm_threads_current{} 650.0
    *jvm_threads_daemon{} 17.0
    *jvm_threads_peak{} 652.0
    *jvm_threads_started_total{} 866.0
    *jvm_threads_deadlocked{} 0.0
    *jvm_threads_deadlocked_monitor{} 0.0
    *jvm_memory_direct_bytes_used{} 1.3774094351E10
    *jvm_gc_collection_seconds_count{gc="G1 Young Generation"} 37918.0
    *jvm_gc_collection_seconds_sum{gc="G1 Young Generation"} 884.271
    *jvm_gc_collection_seconds_count{gc="G1 Old Generation"} 0.0
    *jvm_gc_collection_seconds_sum{gc="G1 Old Generation"} 0.0
    *jvm_memory_direct_bytes_max{} 2.147483648E10
    //process_cpu_seconds_total{} 4664361.93
    //process_start_time_seconds{} 1.648023061099E9
    *process_open_fds{} 6815.0
    *process_max_fds{} 100001.0
    *process_virtual_memory_bytes{} 9.587154944E10
    *process_resident_memory_bytes{} 5.0382516224E10
     */

    static {
        //TODO  待删除 init 和 max 可以删除 ，保留 used
        names.add("jvm_memory_bytes_committed");
        names.add("jvm_memory_bytes_init");
        names.add("jvm_memory_bytes_max");
        names.add("jvm_memory_direct_bytes_max");
        names.add("jvm_heap_used");
        names.add("jvm_max_memory");

        names.add("jvm_max_direct_memory");
        names.add("jvm_direct_memory_used");

        //找不到 jvm_direct_memory_used 和 jvm_max_direct_memory ,有 jvm_memory_direct_bytes_used
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

        if(sample.labelNames!=null && sample.labelValues!=null){
            int bound= Math.min(sample.labelNames.size(),sample.labelValues.size());
            for(int i=0;i<bound;i++){
                baradMetric.getDimension().put(sample.labelNames.get(i),sample.labelValues.get(i));
            }
        }
        
        //TODO  set component and ip
        return baradMetric;
    }

    @Override
    public boolean canConvert(String name) {
        return names.contains(name);
    }
}
