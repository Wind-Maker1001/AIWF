package com.aiwf.base.service;

import com.aiwf.base.config.AppProperties;
import com.aiwf.base.glue.GlueGateway;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class BackendCapabilitiesService {

    private final GlueGateway glue;
    private final RestClient accelClient;
    private final AppProperties props;

    public BackendCapabilitiesService(
            GlueGateway glue,
            AppProperties props,
            RestClient.Builder restClientBuilder
    ) {
        this.glue = glue;
        this.props = props;

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(sanitizePositive(props.getAccelConnectTimeoutMs(), 3000)));
        requestFactory.setReadTimeout(Duration.ofMillis(sanitizePositive(props.getAccelReadTimeoutMs(), 30000)));

        this.accelClient = restClientBuilder
                .requestFactory(requestFactory)
                .baseUrl(props.getAccelUrl())
                .defaultHeader("Content-Type", MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> snapshot() {
        Map<String, Object> glueCaps;
        try {
            glueCaps = glue.capabilities();
        } catch (RestClientException e) {
            glueCaps = Map.of(
                    "ok", false,
                    "error", defaultIfBlank(e.getMessage(), "glue capabilities unavailable")
            );
        }

        Map<String, Object> accelCaps;
        try {
            accelCaps = accelClient.get()
                    .uri("/capabilities")
                    .retrieve()
                    .body(Map.class);
            if (accelCaps == null) {
                accelCaps = Map.of("ok", false, "error", "accel capabilities empty response");
            }
        } catch (RestClientException e) {
            accelCaps = Map.of(
                    "ok", false,
                    "error", defaultIfBlank(e.getMessage(), "accel capabilities unavailable")
            );
        }

        Map<String, Object> domains = new LinkedHashMap<>();
        domains.put("flow_domains", nestedArray(glueCaps, "capabilities", "flow_domains"));
        domains.put("published_operator_domains", nestedArray(accelCaps, "domains"));
        domains.put("workflow_operator_domains", nestedArray(accelCaps, "workflow_domains"));

        return Map.of(
                "ok", truthy(glueCaps.get("ok")) || truthy(accelCaps.get("ok")),
                "glue", Map.of(
                        "url", props.getGlueUrl(),
                        "snapshot", glueCaps
                ),
                "accel", Map.of(
                        "url", props.getAccelUrl(),
                        "snapshot", accelCaps
                ),
                "domains", domains
        );
    }

    @SuppressWarnings("unchecked")
    private static java.util.List<Object> nestedArray(Map<String, Object> root, String... keys) {
        Object current = root;
        for (String key : keys) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return java.util.List.of();
            }
            current = currentMap.get(key);
        }
        if (current instanceof java.util.List<?> list) {
            return (java.util.List<Object>) list;
        }
        return java.util.List.of();
    }

    private static boolean truthy(Object value) {
        return value instanceof Boolean bool && bool;
    }

    private static int sanitizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

    private static String defaultIfBlank(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }
}
