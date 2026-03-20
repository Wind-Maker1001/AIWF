package com.aiwf.base.glue;

import org.slf4j.Logger;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.function.Supplier;

final class GlueClientSupport {
    private GlueClientSupport() {
    }

    static SimpleClientHttpRequestFactory createRequestFactory(int connectTimeoutMs, int readTimeoutMs) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(sanitizePositive(connectTimeoutMs, 3000)));
        requestFactory.setReadTimeout(Duration.ofMillis(sanitizePositive(readTimeoutMs, 30000)));
        return requestFactory;
    }

    static <T> T executeWithRetry(
            Logger log,
            String method,
            String path,
            int maxAttempts,
            long retryDelayMs,
            Supplier<T> action
    ) {
        int attempts = sanitizePositive(maxAttempts, 1);
        RestClientException lastError = null;
        long startedAt = System.nanoTime();

        for (int attempt = 1; attempt <= attempts; attempt++) {
            long attemptStartedAt = System.nanoTime();
            try {
                T result = action.get();
                log.info(
                        "glue {} {} success attempt={}/{} duration_ms={}",
                        method,
                        path,
                        attempt,
                        attempts,
                        elapsedMillis(attemptStartedAt)
                );
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
                        retryDelayMs,
                        safeMessage(e)
                );
                sleepBeforeRetry(retryDelayMs);
            }
        }

        throw lastError == null
                ? new ResourceAccessException("glue request failed without exception details")
                : lastError;
    }

    static int sanitizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

    static long elapsedMillis(long startedAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedAtNanos).toMillis();
    }

    static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }

    private static void sleepBeforeRetry(long retryDelayMs) {
        if (retryDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResourceAccessException("glue retry interrupted", new java.io.IOException(e));
        }
    }
}
