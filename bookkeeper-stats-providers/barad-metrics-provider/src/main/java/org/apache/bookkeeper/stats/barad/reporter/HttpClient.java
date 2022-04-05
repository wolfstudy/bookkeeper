package org.apache.bookkeeper.stats.barad.reporter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

public class HttpClient {

    private final String host;

    private final OkHttpClient okHttpClient;

    private Monitor monitor;

    public HttpClient(String host, int connectTimeout, int requestTimeout, int connectionNum) {
        this.host = host;

        okHttpClient = new OkHttpClient().newBuilder()
                .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(requestTimeout, TimeUnit.MILLISECONDS)
                .connectionPool(new ConnectionPool(connectionNum, 5, TimeUnit.MINUTES))
                .addInterceptor(chain -> {
                    monitor.addTotalCount(1);
                    return chain.proceed(chain.request());
                })
                .build();
    }

    public void addMonitor(Monitor monitor) {
        this.monitor = monitor;
    }


    public CompletableFuture<Response> doPostAsync(byte[] postData) {
        Request request = new Request.Builder()
                .addHeader("Content-Type", "application/json")
                .url(this.host)
                .post(RequestBody.create(postData,MediaType.parse("application/json")))
                .build();

        CompletableFuture<Response> future = new CompletableFuture<>();
        okHttpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                future.complete(response);
            }
        });

        return future;
    }

    public void close() {

    }
}
