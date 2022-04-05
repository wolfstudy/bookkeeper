
package org.apache.bookkeeper.stats.barad;


import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.bookkeeper.stats.prometheus.PrometheusTextFormatUtil;
import org.apache.commons.configuration.Configuration;

public class BaradMetricsProvider extends PrometheusMetricsProvider {

    private String url;

    private Integer connectionTimeoutMs;

    private Integer batchSizeLimit;

    private Integer connectionNum;

    public BaradMetricsProvider() {
        this(CollectorRegistry.defaultRegistry);
    }

    public BaradMetricsProvider(CollectorRegistry registry) {
        super(registry);
    }

    @Override
    public void start(Configuration conf) {
        super.start(conf);
        // report metric every minute
        this.executor.scheduleAtFixedRate(() -> reportMetricToBarad(),
                0, 1, TimeUnit.MINUTES);
    }

    @Override
    public void stop() {
        super.stop();
    }

    void reportMetricToBarad() {

//       PrometheusTextFormatUtil.writeMetricsCollectedByPrometheusClient(writer, registry);
//
//        gauges.forEach((sc, gauge) -> PrometheusTextFormatUtil.writeGauge(writer, sc.getScope(), gauge));
//        counters.forEach((sc, counter) -> PrometheusTextFormatUtil.writeCounter(writer, sc.getScope(), counter));
//        opStats.forEach((sc, opStatLogger) ->
//                PrometheusTextFormatUtil.writeOpStat(writer, sc.getScope(), opStatLogger));

        BaradReportUtil.reportRegistry(this.registry);
        gauges.forEach((sc, gauge) -> BaradReportUtil.reportGauge(sc.getScope(), gauge));
        counters.forEach((sc, counter) -> BaradReportUtil.reportCounter(sc.getScope(), counter));
        opStats.forEach((sc, opStatLogger) ->
                BaradReportUtil.reportOpStat(sc.getScope(), opStatLogger));

    }


}
