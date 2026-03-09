package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ApiErrorResp(
        boolean ok,
        String error,
        String message,
        Map<String, Object> details
) {
    public ApiErrorResp(String error, String message, Map<String, Object> details) {
        this(false, error, message, details == null || details.isEmpty() ? null : Map.copyOf(details));
    }

    @JsonAnyGetter
    public Map<String, Object> extraFields() {
        return details == null ? Map.of() : details;
    }
}
