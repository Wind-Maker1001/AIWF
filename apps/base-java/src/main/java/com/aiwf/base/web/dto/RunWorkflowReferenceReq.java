package com.aiwf.base.web.dto;

import com.aiwf.base.web.ApiException;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;

public final class RunWorkflowReferenceReq extends FlexibleBody {

    @JsonProperty("published_version_id")
    private String publishedVersionId;

    @JsonProperty("version_id")
    private String versionId;

    @JsonProperty("actor")
    private String actor;

    @JsonProperty("ruleset_version")
    private String rulesetVersion;

    @JsonProperty("params")
    private Object params;

    public String getPublishedVersionId() {
        return publishedVersionId;
    }

    public void setPublishedVersionId(String publishedVersionId) {
        this.publishedVersionId = publishedVersionId;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

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

    public String resolvedVersionId() {
        String normalizedVersionId = normalizeIdentifier(versionId);
        String normalizedPublishedVersionId = normalizeIdentifier(publishedVersionId);
        if (!normalizedVersionId.isEmpty() && !normalizedPublishedVersionId.isEmpty()) {
            throw ApiException.badRequest(
                    "workflow_reference_request_invalid",
                    "provide either published_version_id or version_id, not both",
                    Map.of(
                            "published_version_id", normalizedPublishedVersionId,
                            "version_id", normalizedVersionId
                    )
            );
        }
        String resolved = !normalizedVersionId.isEmpty() ? normalizedVersionId : normalizedPublishedVersionId;
        if (resolved.isEmpty()) {
            throw ApiException.badRequest(
                    "workflow_reference_request_invalid",
                    "published_version_id or version_id is required"
            );
        }
        if (containsForbiddenWorkflowPayload()) {
            throw ApiException.badRequest(
                    "workflow_reference_request_invalid",
                    "workflow reference request must not include workflow_definition, graph, or flow",
                    Map.of("forbidden_fields", forbiddenWorkflowPayloadFields())
            );
        }
        return resolved;
    }

    public Map<String, Object> resolvedParams() {
        Map<String, Object> out = new LinkedHashMap<>();
        if (params instanceof Map<?, ?> paramMap) {
            for (Map.Entry<?, ?> entry : paramMap.entrySet()) {
                if (entry.getKey() != null) {
                    out.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
        } else {
            out.putAll(extras());
        }
        return out;
    }

    private boolean containsForbiddenWorkflowPayload() {
        return !forbiddenWorkflowPayloadFields().isEmpty();
    }

    private java.util.List<String> forbiddenWorkflowPayloadFields() {
        java.util.List<String> fields = new java.util.ArrayList<>();
        Map<String, Object> extras = extras();
        if (extras.containsKey("workflow_definition")) fields.add("workflow_definition");
        if (extras.containsKey("graph")) fields.add("graph");
        if (extras.containsKey("flow")) fields.add("flow");
        return fields;
    }

    private String normalize(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    private String normalizeIdentifier(String value) {
        return value == null ? "" : value.trim();
    }
}
