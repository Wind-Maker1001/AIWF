package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public final class StepFailCallbackReq extends FlexibleBody {

    @JsonProperty("error")
    private String error;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String errorOrDefault(String fallback) {
        if (error == null) {
            return fallback;
        }
        String trimmed = error.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    public Map<String, Object> payload(String fallbackError) {
        Map<String, Object> out = new LinkedHashMap<>();
        putIfNotNull(out, "error", errorOrDefault(fallbackError));
        out.putAll(extras());
        return out;
    }
}
