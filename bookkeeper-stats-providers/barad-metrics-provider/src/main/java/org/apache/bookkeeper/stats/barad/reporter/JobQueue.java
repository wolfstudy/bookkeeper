package org.apache.bookkeeper.stats.barad.reporter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Builder;
import lombok.Data;

public class JobQueue {

    private final LinkedList<Job> backingQueue = new LinkedList<>();

    public JobQueue() {

    }


    public List<Job> poll(int maxSize) {
        synchronized (backingQueue) {
            int fetched = 0;
            Job job;
            List<Job> resultSet = new ArrayList<>(maxSize);
            while (fetched < maxSize
                    && ((job = backingQueue.poll()) != null)) {
                fetched += job.getMetrics().size();
                resultSet.add(job);
            }
            return resultSet;
        }

    }


    public void add(Job job) {
        backingQueue.add(job);
    }


    @Data
    @Builder
    public static class Job {

        private List<BaradMetric> metrics;

        private CompletableFuture<?> notifyFuture;

    }

}
