package org.apache.bookkeeper.stats.barad.reporter;

import com.google.common.hash.Hashing;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class BaradMetric {

    private String name;
    private BigDecimal value;
    private Integer timestamp;
    private String namespace = "qce/tdmq";
    private Map<String, String> dimension = new HashMap<>();

    public BaradMetric(String name) {
        this.name = name;
        this.timestamp = (int) (System.currentTimeMillis() / 1000L);
    }

    public String computeKey() {
        StringBuilder builder = new StringBuilder();
        dimension.forEach((k, v) -> {
            builder.append("|")
                    .append(v);
        });
        // TODO is equal DigestUtils.md5Hex(java.lang.String) ?
        return name + "|" + Hashing.sha256().hashUnencodedChars(builder.toString()).toString();
    }

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", dimension=" + dimension +
                ", timestamp=" + timestamp +
                '}';
    }
}
