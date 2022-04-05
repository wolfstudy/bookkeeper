package org.apache.bookkeeper.stats.barad.reporter;

import java.util.List;

public interface Reporter <T> {

    void report(List<T> metrics);

    void shutdown();
}