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
        this.client = restClientBuilder
                .requestFactory(GlueClientSupport.createRequestFactory(
                        props.getGlueConnectTimeoutMs(),
                        props.getGlueReadTimeoutMs()
                ))
                .baseUrl(props.getGlueUrl())
                .build();
        this.glueRunMaxAttempts = GlueClientSupport.sanitizePositive(props.getGlueRunMaxAttempts(), 1);
        this.glueHealthMaxAttempts = GlueClientSupport.sanitizePositive(props.getGlueHealthMaxAttempts(), 3);
        this.glueRetryDelayMs = Math.max(0L, props.getGlueRetryDelayMs());
    }

    @Override
    @SuppressWarnings("unchecked")
    public GlueRunResult runFlow(String jobId, String flow, GlueRunFlowReq request) {
        String path = "/jobs/%s/run/%s".formatted(jobId, flow);
        // Flow dispatch is not idempotent. Retrying a POST here can execute the same job twice.
        Map<String, Object> response = executeWithRetry(
                "POST",
                path,
                1,
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
        return GlueClientSupport.executeWithRetry(
                log,
                method,
                path,
                maxAttempts,
                glueRetryDelayMs,
                action
        );
    }
}
