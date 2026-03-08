package com.aiwf.base.web;

import org.springframework.http.HttpStatus;

import java.util.Map;

public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final String error;
    private final Map<String, Object> details;

    public ApiException(HttpStatus status, String error, String message, Map<String, Object> details) {
        super(message);
        this.status = status;
        this.error = error;
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

    public HttpStatus getStatus() {
        return status;
    }

    public String getError() {
        return error;
    }

    public Map<String, Object> getDetails() {
        return details;
    }

    public static ApiException badRequest(String error, String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, error, message, Map.of());
    }

    public static ApiException badRequest(String error, String message, Map<String, Object> details) {
        return new ApiException(HttpStatus.BAD_REQUEST, error, message, details);
    }

    public static ApiException notFound(String error, String message, Map<String, Object> details) {
        return new ApiException(HttpStatus.NOT_FOUND, error, message, details);
    }

    public static ApiException serviceUnavailable(String error, String message) {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, error, message, Map.of());
    }
}
