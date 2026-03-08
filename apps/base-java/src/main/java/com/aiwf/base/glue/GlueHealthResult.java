package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public final class GlueHealthResult {

    private final boolean ok;
    private final String error;
    private final Map<String, Object> extras;

    public GlueHealthResult(boolean ok, String error, Map<String, Object> extras) {
        this.ok = ok;
        this.error = error;
        this.extras = extras == null ? Map.of() : Map.copyOf(extras);
    }

    public boolean isOk() {
        return ok;
    }

    public String getError() {
        return error;
    }

    @JsonAnyGetter
    public Map<String, Object> extras() {
        return extras;
    }

    public static GlueHealthResult fromMap(Map<String, Object> body) {
        Map<String, Object> source = body == null ? Map.of() : new LinkedHashMap<>(body);
        boolean ok = Boolean.TRUE.equals(source.remove("ok"));
        String error = stringOrNull(source.remove("error"));
        return new GlueHealthResult(ok, error, source);
    }

    public static GlueHealthResult unavailable(String error) {
        return new GlueHealthResult(false, error, Map.of());
    }

    private static String stringOrNull(Object value) {
        return value == null ? null : String.valueOf(value);
    }
}
