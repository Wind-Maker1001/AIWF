package com.aiwf.base.glue;

import com.aiwf.base.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

@Component
public class GlueClient implements GlueGateway {
    private static final Logger log = LoggerFactory.getLogger(GlueClient.class);

    private final RestClient client;
    private final int glueRunMaxAttempts;
    private final int glueHealthMaxAttempts;
    private final long glueRetryDelayMs;

    public GlueClient(AppProperties props, RestClient.Builder restClientBuilder) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(sanitizePositive(props.getGlueConnectTimeoutMs(), 3000)));
        requestFactory.setReadTimeout(Duration.ofMillis(sanitizePositive(props.getGlueReadTimeoutMs(), 30000)));

        this.client = restClientBuilder
                .requestFactory(requestFactory)
                .baseUrl(props.getGlueUrl())
                .build();
        this.glueRunMaxAttempts = sanitizePositive(props.getGlueRunMaxAttempts(), 1);
        this.glueHealthMaxAttempts = sanitizePositive(props.getGlueHealthMaxAttempts(), 3);
        this.glueRetryDelayMs = Math.max(0L, props.getGlueRetryDelayMs());
    }

    @Override
    @SuppressWarnings("unchecked")
    public GlueRunResult runFlow(String jobId, String flow, GlueRunFlowReq request) {
        String path = "/jobs/%s/run/%s".formatted(jobId, flow);
        Map<String, Object> response = executeWithRetry(
                "POST",
                path,
                glueRunMaxAttempts,
                () -> client.post()
                        .uri("/jobs/{jobId}/run/{flow}", jobId, flow)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(request == null ? Map.of() : request)
                        .retrieve()
                        .body(Map.class)
        );
        return GlueRunFlowResp.fromMap(response, jobId, flow).toRunResult();
    }

    @Override
    @SuppressWarnings("unchecked")
    public GlueHealthResult health() {
        Map<String, Object> response = executeWithRetry(
                "GET",
                "/health",
                glueHealthMaxAttempts,
                () -> client.get().uri("/health").retrieve().body(Map.class)
        );
        return GlueHealthResult.fromMap(response);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> capabilities() {
        return executeWithRetry(
                "GET",
                "/capabilities",
                glueHealthMaxAttempts,
                () -> client.get().uri("/capabilities").retrieve().body(Map.class)
        );
    }

    private <T> T executeWithRetry(String method, String path, int maxAttempts, Supplier<T> action) {
        int attempts = sanitizePositive(maxAttempts, 1);
        RestClientException lastError = null;
        long startedAt = System.nanoTime();

        for (int attempt = 1; attempt <= attempts; attempt++) {
            long attemptStartedAt = System.nanoTime();
            try {
                T result = action.get();
                log.info("glue {} {} success attempt={}/{} duration_ms={}", method, path, attempt, attempts, elapsedMillis(attemptStartedAt));
                return result;
            } catch (RestClientException e) {
                lastError = e;
                long attemptElapsedMs = elapsedMillis(attemptStartedAt);
                if (attempt >= attempts) {
                    log.error(
                            "glue {} {} failed attempt={}/{} duration_ms={} total_duration_ms={} error={}",
                            method,
                            path,
                            attempt,
                            attempts,
                            attemptElapsedMs,
                            elapsedMillis(startedAt),
                            safeMessage(e),
                            e
                    );
                    throw e;
                }

                log.warn(
                        "glue {} {} failed attempt={}/{} duration_ms={} retry_in_ms={} error={}",
                        method,
                        path,
                        attempt,
                        attempts,
                        attemptElapsedMs,
                        glueRetryDelayMs,
                        safeMessage(e)
                );
                sleepBeforeRetry();
            }
        }

        throw lastError == null ? new ResourceAccessException("glue request failed without exception details") : lastError;
    }

    private void sleepBeforeRetry() {
        if (glueRetryDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(glueRetryDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResourceAccessException("glue retry interrupted", new java.io.IOException(e));
        }
    }

    private static int sanitizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

    private static long elapsedMillis(long startedAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
