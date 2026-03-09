package com.aiwf.base.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JsonUtil {
    private static final Logger log = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper M = new ObjectMapper();
    private JsonUtil(){}

    public static String toJson(Object o) {
        try {
            return M.writeValueAsString(o);
        } catch (Exception e) {
            log.warn("json serialization failed type={} error={}", typeOf(o), safeMessage(e));
            return "{}";
        }
    }

    public static String toJsonOrNull(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof String s) {
            return s;
        }
        return toJson(o);
    }

    private static String typeOf(Object o) {
        return o == null ? "null" : o.getClass().getName();
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
