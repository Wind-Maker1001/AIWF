package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record GlueRunFlowResp(
        boolean ok,
        @JsonProperty("job_id") String jobId,
        String flow,
        Double seconds,
        @JsonProperty("output_hash") String outputHash,
        List<Map<String, Object>> artifacts,
        Map<String, Object> profile,
        Map<String, Object> accel,
        String error,
        @JsonProperty("error_id") String errorId,
        Map<String, Object> extras
) {
    @SuppressWarnings("unchecked")
    public static GlueRunFlowResp fromMap(Map<String, Object> body, String defaultJobId, String defaultFlow) {
        Map<String, Object> source = body == null ? new LinkedHashMap<>() : new LinkedHashMap<>(body);
        boolean ok = Boolean.TRUE.equals(source.remove("ok"));
        String jobId = stringOrDefault(source.remove("job_id"), defaultJobId);
        String flow = stringOrDefault(source.remove("flow"), defaultFlow);
        Double seconds = doubleOrNull(source.remove("seconds"));
        String outputHash = stringOrNull(source.remove("output_hash"));
        List<Map<String, Object>> artifacts = source.containsKey("artifacts") ? (List<Map<String, Object>>) source.remove("artifacts") : null;
        Map<String, Object> profile = source.containsKey("profile") ? (Map<String, Object>) source.remove("profile") : null;
        Map<String, Object> accel = source.containsKey("accel") ? (Map<String, Object>) source.remove("accel") : null;
        String error = stringOrNull(source.remove("error"));
        String errorId = stringOrNull(source.remove("error_id"));
        return new GlueRunFlowResp(ok, jobId, flow, seconds, outputHash, artifacts, profile, accel, error, errorId, source);
    }

    public GlueRunResult toRunResult() {
        Map<String, Object> out = new LinkedHashMap<>();
        if (seconds != null) {
            out.put("seconds", seconds);
        }
        if (outputHash != null) {
            out.put("output_hash", outputHash);
        }
        if (artifacts != null) {
            out.put("artifacts", artifacts);
        }
        if (profile != null) {
            out.put("profile", profile);
        }
        if (accel != null) {
            out.put("accel", accel);
        }
        if (errorId != null) {
            out.put("error_id", errorId);
        }
        if (extras != null && !extras.isEmpty()) {
            out.putAll(extras);
        }
        return new GlueRunResult(ok, jobId, flow, error, out);
    }

    private static String stringOrDefault(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    private static String stringOrNull(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Double doubleOrNull(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
