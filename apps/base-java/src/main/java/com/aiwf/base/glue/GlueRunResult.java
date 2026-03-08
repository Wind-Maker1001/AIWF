package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class GlueRunResult {

    private final boolean ok;
    private final String jobId;
    private final String flow;
    private final String error;
    private final Map<String, Object> extras;

    public GlueRunResult(boolean ok, String jobId, String flow, String error, Map<String, Object> extras) {
        this.ok = ok;
        this.jobId = jobId;
        this.flow = flow;
        this.error = error;
        this.extras = extras == null ? Map.of() : Map.copyOf(extras);
    }

    public boolean isOk() {
        return ok;
    }

    @JsonProperty("job_id")
    public String getJobId() {
        return jobId;
    }

    public String getFlow() {
        return flow;
    }

    public String getError() {
        return error;
    }

    @JsonAnyGetter
    public Map<String, Object> extras() {
        return extras;
    }

    public static GlueRunResult fromMap(Map<String, Object> body, String defaultJobId, String defaultFlow) {
        Map<String, Object> source = body == null ? Map.of() : new LinkedHashMap<>(body);
        boolean ok = Boolean.TRUE.equals(source.remove("ok"));
        String jobId = stringOrDefault(source.remove("job_id"), defaultJobId);
        String flow = stringOrDefault(source.remove("flow"), defaultFlow);
        String error = stringOrNull(source.remove("error"));
        return new GlueRunResult(ok, jobId, flow, error, source);
    }

    public static GlueRunResult failed(String jobId, String flow, String error) {
        return new GlueRunResult(false, jobId, flow, error, Map.of());
    }

    private static String stringOrDefault(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    private static String stringOrNull(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
