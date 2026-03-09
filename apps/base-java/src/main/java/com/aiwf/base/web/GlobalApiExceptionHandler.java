package com.aiwf.base.web;

import com.aiwf.base.web.dto.ApiErrorResp;
import jakarta.validation.ConstraintViolationException;
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

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiErrorResp> handleApiException(ApiException e) {
        return ResponseEntity.status(e.getStatus())
                .body(new ApiErrorResp(e.getError(), e.getMessage(), e.getDetails()));
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ApiErrorResp> handleResponseStatus(ResponseStatusException e) {
        HttpStatus status = HttpStatus.valueOf(e.getStatusCode().value());
        String error = status == HttpStatus.NOT_FOUND ? "not_found" : "bad_request";
        return ResponseEntity.status(status)
                .body(new ApiErrorResp(error, e.getReason(), Map.of()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResp> handleValidation(MethodArgumentNotValidException e) {
        FieldError fieldError = e.getBindingResult().getFieldErrors().stream().findFirst().orElse(null);
        String message = fieldError == null ? "validation failed" : fieldError.getDefaultMessage();
        Map<String, Object> details = fieldError == null ? Map.of() : Map.of("field", fieldError.getField());
        return ResponseEntity.badRequest()
                .body(new ApiErrorResp("validation_error", message, details));
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<ApiErrorResp> handleDataAccess(DataAccessException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiErrorResp(
                        "data_store_unavailable",
                        "data store unavailable",
                        Map.of("cause", String.valueOf(e.getMostSpecificCause()))
                ));
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, ConstraintViolationException.class})
    public ResponseEntity<ApiErrorResp> handleBadRequest(Exception e) {
        return ResponseEntity.badRequest()
                .body(new ApiErrorResp("bad_request", "invalid request body", Map.of()));
    }
}
