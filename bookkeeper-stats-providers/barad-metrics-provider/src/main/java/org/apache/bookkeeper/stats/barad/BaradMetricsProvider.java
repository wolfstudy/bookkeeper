
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

    public static final String BARAD_URL = "baradUrl";

    private BaradMetricsReporter metricsReporter;

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
        MetricsReporterConfig reporterConfig = new MetricsReporterConfig();
        reporterConfig.setBaradUrl(conf.getString(BARAD_URL));
        log.info("barad report config {}",reporterConfig);
        metricsReporter = new BaradMetricsReporter(reporterConfig);
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
            metricsReporter.reportGaugeMap(gauges);
            metricsReporter.reportCounterMap(counters);
            opStats.forEach((sc, opStatLogger) ->
                    metricsReporter.reportOpStat(sc.getScope(), opStatLogger));
        } catch (Throwable t) {
            log.error("report to barad error ", t);
        }
    }


}
