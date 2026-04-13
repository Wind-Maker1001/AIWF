package com.aiwf.base.web;

import com.aiwf.base.web.dto.ApiErrorResp;
import jakarta.validation.ConstraintViolationException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;

@RestControllerAdvice
public class GlobalApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalApiExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiErrorResp> handleApiException(ApiException e, HttpServletRequest request) {
        logForStatus(e.getStatus(), request, e.getError(), e);
        return ResponseEntity.status(e.getStatus())
                .body(new ApiErrorResp(e.getError(), e.getMessage(), e.getDetails()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiErrorResp> handleResponseStatus(ResponseStatusException e, HttpServletRequest request) {
        HttpStatus status = HttpStatus.valueOf(e.getStatusCode().value());
        String error = status == HttpStatus.NOT_FOUND ? "not_found" : "bad_request";
        logForStatus(status, request, error, e);
        return ResponseEntity.status(status)
                .body(new ApiErrorResp(error, e.getReason(), Map.of()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResp> handleValidation(MethodArgumentNotValidException e, HttpServletRequest request) {
        FieldError fieldError = e.getBindingResult().getFieldErrors().stream().findFirst().orElse(null);
        String message = fieldError == null ? "validation failed" : fieldError.getDefaultMessage();
        Map<String, Object> details = fieldError == null ? Map.of() : Map.of("field", fieldError.getField());
        log.warn("request validation failed method={} path={} field={} error={}", requestMethod(request), requestPath(request), fieldError == null ? null : fieldError.getField(), message);
        return ResponseEntity.badRequest()
                .body(new ApiErrorResp("validation_error", message, details));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiErrorResp> handleDataAccess(DataAccessException e, HttpServletRequest request) {
        log.error("data access failed method={} path={} error={}", requestMethod(request), requestPath(request), safeMessage(e), e);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiErrorResp(
                        "data_store_unavailable",
                        "data store unavailable",
                        Map.of()
                ));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, ConstraintViolationException.class})
    public ResponseEntity<ApiErrorResp> handleBadRequest(Exception e, HttpServletRequest request) {
        log.warn("bad request body method={} path={} error={}", requestMethod(request), requestPath(request), safeMessage(e));
        return ResponseEntity.badRequest()
                .body(new ApiErrorResp("bad_request", "invalid request body", Map.of()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResp> handleUnexpected(Exception e, HttpServletRequest request) {
        log.error("unexpected server error method={} path={} error={}", requestMethod(request), requestPath(request), safeMessage(e), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiErrorResp("internal_error", "internal server error", Map.of()));
    }

    private void logForStatus(HttpStatus status, HttpServletRequest request, String error, Exception e) {
        if (status.is5xxServerError()) {
            log.error("api exception method={} path={} status={} error={} message={}", requestMethod(request), requestPath(request), status.value(), error, e.getMessage(), e);
        } else {
            log.warn("api exception method={} path={} status={} error={} message={}", requestMethod(request), requestPath(request), status.value(), error, e.getMessage());
        }
    }

    private static String requestMethod(HttpServletRequest request) {
        return request == null ? "unknown" : request.getMethod();
    }

    private static String requestPath(HttpServletRequest request) {
        return request == null ? "unknown" : request.getRequestURI();
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
