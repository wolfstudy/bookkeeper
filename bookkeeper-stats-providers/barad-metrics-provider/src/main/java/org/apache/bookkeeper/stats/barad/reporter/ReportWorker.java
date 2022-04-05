package org.apache.bookkeeper.stats.barad.reporter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReportWorker implements Runnable {

    private volatile boolean isRunning = true;

    private final int batchLimit;
    private final JobQueue jobQueue;
    private final HttpClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Response {

        private Integer returnValue;
        private Integer returnCode;
        private String msg;
        private BigDecimal seq;
    }

    public ReportWorker(HttpClient client, int batchLimit, JobQueue jobQueue) {
        this.client = client;
        this.batchLimit = batchLimit;
        this.jobQueue = jobQueue;
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void terminate() {
        this.isRunning = false;
        this.client.close();
    }

    private CompletableFuture<Void> reportMetricsAsync(List<BaradMetric> uploadMetrics) {
        CompletableFuture<Void> newFuture = new CompletableFuture<>();
        try {
            byte[] requestBody = mapper.writeValueAsBytes(uploadMetrics);
            CompletableFuture<okhttp3.Response> future = client.doPostAsync(requestBody);
            future.whenComplete((response, e) -> {
                if (response != null) {
                    try {
                        Response data = mapper.readValue(response.body().string(), Response.class);
                        if (data.getReturnValue() != 0) {
                            log.error("Barad error response {}", response);
                        }
                    } catch (Exception ex) {
                        log.error("Barad response exception", ex);
                    }
                }
                newFuture.complete(null);
            });
        } catch (Exception e) {
            log.error("reportMetricsAsync exception.", e);
            newFuture.complete(null);
        }

        return newFuture;

    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                List<JobQueue.Job> jobs = jobQueue.poll(batchLimit);
                if (jobs != null && !jobs.isEmpty()) {
                    List<BaradMetric> metrics = new ArrayList<>();
                    jobs.forEach(job -> metrics.addAll(job.getMetrics()));
                    completeJobs(jobs, null);
                    int batchIndex = 0;
                    int batchCount = 1000;
                    log.debug("Send metrics in {} batches,", metrics.size() / batchCount);
                    List<CompletableFuture<?>> futures = new ArrayList<>();
                    while (batchIndex < metrics.size()) {
                        int endIndex = batchIndex + batchCount;
                        if (endIndex > metrics.size()) {
                            endIndex = metrics.size();
                        }
                        List<BaradMetric> uploadMetrics = metrics.subList(batchIndex, endIndex);
                        futures.add(reportMetricsAsync(uploadMetrics));
                        batchIndex += batchCount;
                    }
                    CompletableFuture<?>[] futureArray = new CompletableFuture[futures.size()];
                    futures.toArray(futureArray);
                    CompletableFuture<Void> allCompleteFuture = CompletableFuture.allOf(futureArray);
                    allCompleteFuture.join();
                } else {
                    sleep(500);
                }
            }
        } catch (Throwable t) {
            log.error("Worker quit for {}", t.getMessage(), t);
        } finally {
            log.info("Bye");
            isRunning = false;
        }
    }

    private void completeJobs(List<JobQueue.Job> jobs, Throwable e) {
        jobs.stream().filter(job -> job.getNotifyFuture() != null)
                .forEach(job -> {
                    if (e != null) {
                        job.getNotifyFuture().completeExceptionally(e);
                    } else {
                        job.getNotifyFuture().complete(null);
                    }
                });
    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            //
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

}
