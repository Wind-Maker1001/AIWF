package com.aiwf.base.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

final class GlueRunParamsSupport {
    private GlueRunParamsSupport() {
    }

    static Map<String, Object> filterReservedKeys(Map<String, Object> params, Set<String> reservedKeys) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (params == null || params.isEmpty()) {
            return out;
        }
        params.forEach((key, value) -> {
            if (!reservedKeys.contains(key)) {
                out.put(key, value);
            }
        });
        return out;
    }
}
