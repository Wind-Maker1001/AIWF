package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record JobRunRecordResp(
        @JsonProperty("schema_version") String schemaVersion,
        String owner,
        @JsonProperty("source_of_truth") String sourceOfTruth,
        @JsonProperty("run_id") String runId,
        @JsonProperty("ts") Object ts,
        @JsonProperty("run_request_kind") String runRequestKind,
        @JsonProperty("version_id") String versionId,
        @JsonProperty("published_version_id") String publishedVersionId,
        @JsonProperty("workflow_definition_source") String workflowDefinitionSource,
        @JsonProperty("workflow_id") String workflowId,
        String status,
        boolean ok,
        Map<String, Object> payload,
        Map<String, Object> config,
        Map<String, Object> result
) {
}
