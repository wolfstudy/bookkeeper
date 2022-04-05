package org.apache.bookkeeper.stats.barad.reporter;

import lombok.Data;

@Data
public class MetricsReporterConfig {

    private String url;

    private Integer connectionTimeoutMs = 10 * 000;

    private Integer batchSizeLimit = 100;

    private Integer connectionNum = 3;

}
