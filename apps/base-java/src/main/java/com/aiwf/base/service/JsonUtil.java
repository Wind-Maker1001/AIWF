package com.aiwf.base.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

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

    public static Object fromJson(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return M.readValue(raw, Object.class);
        } catch (Exception e) {
            log.warn("json parse failed error={}", safeMessage(e));
            return raw;
        }
    }

    public static Map<String, Object> fromJsonObject(String raw) {
        Object value = fromJson(raw);
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() != null) {
                    out.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return out;
        }
        return Map.of();
    }

    private static String typeOf(Object o) {
        return o == null ? "null" : o.getClass().getName();
    }

    private static String safeMessage(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }
}
