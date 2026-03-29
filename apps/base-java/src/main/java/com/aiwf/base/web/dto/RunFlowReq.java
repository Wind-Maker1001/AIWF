package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public final class RunFlowReq extends FlexibleBody {

    @JsonProperty("actor")
    private String actor;

    @JsonProperty("ruleset_version")
    private String rulesetVersion;

    @JsonProperty("params")
    private Object params;

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public String getRulesetVersion() {
        return rulesetVersion;
    }

    public void setRulesetVersion(String rulesetVersion) {
        this.rulesetVersion = rulesetVersion;
    }

    public Object getParams() {
        return params;
    }

    public void setParams(Object params) {
        this.params = params;
    }

    public String actorOrDefault(String fallback) {
        return normalize(actor, fallback);
    }

    public String rulesetVersionOrDefault(String fallback) {
        return normalize(rulesetVersion, fallback);
    }

    public Map<String, Object> resolvedParams() {
        if (params instanceof Map<?, ?> paramMap) {
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : paramMap.entrySet()) {
                if (entry.getKey() != null) {
                    out.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return out;
        }
        return extras();
    }

    public boolean containsReferenceFields() {
        Map<String, Object> extras = extras();
        return extras.containsKey("version_id")
                || extras.containsKey("published_version_id")
                || extras.containsKey("workflow_definition")
                || extras.containsKey("graph")
                || extras.containsKey("flow");
    }

    private String normalize(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }
}
