package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DifyRunCleaningReq(
        @JsonProperty("owner") String owner,
        @JsonProperty("actor") String actor,
        @JsonProperty("ruleset_version") String rulesetVersion,
        @JsonProperty("policy") Object policy,
        @JsonProperty("params") Object params
) {
    public String ownerOrDefault(String fallback) {
        return normalize(owner, fallback);
    }

    public String actorOrDefault(String fallback) {
        return normalize(actor, fallback);
    }

    public String rulesetVersionOrDefault(String fallback) {
        return normalize(rulesetVersion, fallback);
    }

    public Map<String, Object> policyOrEmpty() {
        return copyMap(policy);
    }

    public Map<String, Object> paramsOrEmpty() {
        return copyMap(params);
    }

    private String normalize(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty() || "null".equalsIgnoreCase(trimmed)) {
            return fallback;
        }
        return trimmed;
    }

    private Map<String, Object> copyMap(Object value) {
        if (!(value instanceof Map<?, ?> source)) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (entry.getKey() != null) {
                out.put(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        return out;
    }
}
