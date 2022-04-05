package org.apache.bookkeeper.stats.barad.reporter;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Reporter <T> {

    void report(List<T> metrics, CompletableFuture<?> future);

    void shutdown();
}