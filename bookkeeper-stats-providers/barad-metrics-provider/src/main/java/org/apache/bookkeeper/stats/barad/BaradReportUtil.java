package org.apache.bookkeeper.stats.barad;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.Enumeration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.barad.convert.CounterConverter;
import org.apache.bookkeeper.stats.barad.convert.GaugeConverter;
import org.apache.bookkeeper.stats.barad.convert.OpStatConverter;
import org.apache.bookkeeper.stats.barad.convert.SampleConverter;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetric;
import org.apache.bookkeeper.stats.prometheus.DataSketchesOpStatsLogger;
import org.apache.bookkeeper.stats.prometheus.LongAdderCounter;
import org.apache.bookkeeper.stats.prometheus.SimpleGauge;

@Slf4j
public class BaradReportUtil {
    
    private static CounterConverter counterConverter=new CounterConverter();
    private static GaugeConverter gaugeConverter=new GaugeConverter();
    private static OpStatConverter opStatConverter=new OpStatConverter();
    private static SampleConverter sampleConverter=new SampleConverter();
    
    public static void reportRegistry(CollectorRegistry registry) {

        Enumeration<MetricFamilySamples> metricFamilySamples = registry.metricFamilySamples();
        while (metricFamilySamples.hasMoreElements()) {
            MetricFamilySamples metricFamily = metricFamilySamples.nextElement();

            for (int i = 0; i < metricFamily.samples.size(); i++) {
                Sample sample = metricFamily.samples.get(i);
                reportSample(sample);
            }
        }
    }

    public static void reportSample(Sample sample) {
        if(sampleConverter.canConvert(sample.name)){
            BaradMetric baradMetric =sampleConverter.convert(sample.name,sample);
            log.info("report {}", baradMetric);
        }else {
            log.warn("ignore [{}] report to barad",sample.name);
        }
        
    }

    public static void reportGauge(String name, SimpleGauge<? extends Number> gauge) {
        if(gaugeConverter.canConvert(name)){
            BaradMetric baradMetric =gaugeConverter.convert(name,gauge);
            log.info("report {}", baradMetric);
        }else {
            log.warn("ignore [{}] report to barad",name);
        }
    }

    public static void reportCounter(String name, LongAdderCounter counter) {
        if(counterConverter.canConvert(name)){
            BaradMetric baradMetric =counterConverter.convert(name,counter);
            log.info("report {}", baradMetric);
        }else {
            log.warn("ignore [{}] report to barad",name);
        }
    }

    public static void reportOpStat(String name, DataSketchesOpStatsLogger opStat) {
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
        if(opStatConverter.canConvert(name)){
            List<BaradMetric> baradMetrics =opStatConverter.multiConvert(name,opStat);
            for(BaradMetric metric:baradMetrics) {
                log.info("report {}", metric);
            }
        }else {
            log.warn("ignore [{}] report to barad",name);
        }
    }

}
