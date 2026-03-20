package com.aiwf.base.service;

import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class BackendCapabilitiesSupport {
    private BackendCapabilitiesSupport() {
    }

    static SimpleClientHttpRequestFactory createRequestFactory(int connectTimeoutMs, int readTimeoutMs) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofMillis(sanitizePositive(connectTimeoutMs, 3000)));
        requestFactory.setReadTimeout(Duration.ofMillis(sanitizePositive(readTimeoutMs, 30000)));
        return requestFactory;
    }

    @SuppressWarnings("unchecked")
    static List<Object> nestedArray(Map<String, Object> root, String... keys) {
        Object current = root;
        for (String key : keys) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return List.of();
            }
            current = currentMap.get(key);
        }
        if (current instanceof List<?> list) {
            return (List<Object>) list;
        }
        return List.of();
    }

    static boolean truthy(Object value) {
        return value instanceof Boolean bool && bool;
    }

    static int sanitizePositive(int value, int fallback) {
        return value > 0 ? value : fallback;
    }

    static String defaultIfBlank(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    static Map<String, Object> unavailableSnapshot(String message, String fallback) {
        return Map.of(
                "ok", false,
                "error", defaultIfBlank(message, fallback)
        );
    }

    static Map<String, Object> buildSnapshot(
            String glueUrl,
            Map<String, Object> glueCaps,
            String accelUrl,
            Map<String, Object> accelCaps
    ) {
        Map<String, Object> domains = new LinkedHashMap<>();
        domains.put("flow_domains", nestedArray(glueCaps, "capabilities", "flow_domains"));
        domains.put("published_operator_domains", nestedArray(accelCaps, "domains"));
        domains.put("workflow_operator_domains", nestedArray(accelCaps, "workflow_domains"));

        return Map.of(
                "ok", truthy(glueCaps.get("ok")) || truthy(accelCaps.get("ok")),
                "glue", Map.of(
                        "url", glueUrl,
                        "snapshot", glueCaps
                ),
                "accel", Map.of(
                        "url", accelUrl,
                        "snapshot", accelCaps
                ),
                "domains", domains
        );
    }
}
