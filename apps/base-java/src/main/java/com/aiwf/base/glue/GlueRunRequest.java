package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public record GlueRunRequest(
        @JsonProperty("actor") String actor,
        @JsonProperty("ruleset_version") String rulesetVersion,
        @JsonProperty("params") Map<String, Object> params
) {
    public Map<String, Object> toPayload() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("actor", actor);
        out.put("ruleset_version", rulesetVersion);
        if (params != null) {
            out.put("params", params);
        }
        return out;
    }
}
