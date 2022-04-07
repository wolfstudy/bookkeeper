package org.apache.bookkeeper.stats.barad.reporter;

import lombok.extern.slf4j.Slf4j;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BaradHttpClient {

    private final String host;

    private final OkHttpClient okHttpClient;

    public BaradHttpClient(String host, int connectTimeout, int requestTimeout) {
        this.host = host;

        okHttpClient = new OkHttpClient().newBuilder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .build();
    }

    public Response doPost(byte[] postData) throws IOException, InterruptedException {
        int maxRetryTimes = 3;
        for (int i = 0; i < maxRetryTimes; i++) {
            try {
                Request request = new Request.Builder()
                        .addHeader("Content-Type", "application/json")
                        .url(this.host)
                        .post(RequestBody.create(postData, MediaType.parse("application/json")))
                        .build();
                return okHttpClient.newCall(request).execute();
            } catch (IOException e) {
                if (i == maxRetryTimes - 1) {
                    log.error("report barad unexpected exception retry {} times", i, e);
                } else {
                    Thread.sleep(500 * i);
                }
            }
        }

        throw new RuntimeException("report barad unexpected exception after retry 3 times.");
    }
}
