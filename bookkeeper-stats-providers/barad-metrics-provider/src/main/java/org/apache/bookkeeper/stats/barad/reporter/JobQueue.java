package org.apache.bookkeeper.stats.barad.reporter;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.Builder;
import lombok.Data;

public class JobQueue {

    private final LinkedList<Job> backingQueue = new LinkedList<>();

    private final static int MAX_QUEUE_SIZE = 2000 * 1000;

    public JobQueue() {

    }


    public List<Job> poll(int batchSize) {
        synchronized (backingQueue) {
            int fetched = 0;
            Job job;
            List<Job> resultSet = new ArrayList<>(batchSize);
            while (fetched < batchSize
                    && ((job = backingQueue.poll()) != null)) {
                fetched += job.getMetrics().size();
                resultSet.add(job);
            }
            return resultSet;
        }

    }


    public void add(Job job) {
        if (backingQueue.size() > MAX_QUEUE_SIZE) {
            throw new IllegalArgumentException("backing queue is full, current size " + backingQueue.size());
        }
        backingQueue.add(job);
    }


    @Data
    @Builder
    public static class Job {

        private List<BaradMetric> metrics;

    }

}
