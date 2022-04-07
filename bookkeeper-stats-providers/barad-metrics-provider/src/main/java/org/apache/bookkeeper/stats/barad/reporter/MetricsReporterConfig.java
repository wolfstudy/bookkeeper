package org.apache.bookkeeper.stats.barad.reporter;

import lombok.Data;

@Data
public class MetricsReporterConfig {

    private String baradUrl;

    private Integer connectionTimeoutMs = 10 * 000;

    private Integer batchSizeLimit = 100;

    private Integer connectionNum = 100;

    private Integer queueSize = 1000 * 1000;

}
