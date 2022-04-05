
package org.apache.bookkeeper.stats.barad;

import io.prometheus.client.CollectorRegistry;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.stats.barad.reporter.BaradMetricsReporter;
import org.apache.bookkeeper.stats.barad.reporter.MetricsReporterConfig;
import org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider;
import org.apache.commons.configuration.Configuration;

@Slf4j
public class BaradMetricsProvider extends PrometheusMetricsProvider {


    private BaradMetricsReporter metricsReporter;

    public BaradMetricsProvider() {
        this(CollectorRegistry.defaultRegistry);
    }

    public BaradMetricsProvider(CollectorRegistry registry) {
        super(registry);
        MetricsReporterConfig reporterConfig=new MetricsReporterConfig();
        //todo test url, ip and compant name
        reporterConfig.setUrl("todo");
        metricsReporter = new BaradMetricsReporter(reporterConfig);
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
        try {
            metricsReporter.reportRegistry(this.registry);
            gauges.forEach((sc, gauge) -> metricsReporter.reportGauge(sc.getScope(), gauge));
            counters.forEach((sc, counter) -> metricsReporter.reportCounter(sc.getScope(), counter));
            opStats.forEach((sc, opStatLogger) ->
                    metricsReporter.reportOpStat(sc.getScope(), opStatLogger));
        } catch (Throwable t) {
            log.error("report to barad error ", t);
        }
    }


}
