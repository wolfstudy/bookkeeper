package org.apache.bookkeeper.stats.barad.reporter;

import lombok.Data;

@Data
public class MetricsReporterConfig {

    private String url;

    private Integer connectionTimeoutMs;

    private Integer batchSizeLimit;

    private Integer connectionNum;

}
