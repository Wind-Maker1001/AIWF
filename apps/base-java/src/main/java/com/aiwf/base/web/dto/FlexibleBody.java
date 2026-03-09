package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.LinkedHashMap;
import java.util.Map;

abstract class FlexibleBody {

    @JsonIgnore
    private final Map<String, Object> extras = new LinkedHashMap<>();

    @JsonAnySetter
    public void addExtra(String name, Object value) {
        extras.put(name, value);
    }

    @JsonIgnore
    protected Map<String, Object> extras() {
        return new LinkedHashMap<>(extras);
    }

    protected void putIfNotNull(Map<String, Object> target, String key, Object value) {
        if (value != null) {
            target.put(key, value);
        }
    }
}
