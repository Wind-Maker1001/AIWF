package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record JobRunRecordResp(
        @JsonProperty("schema_version") String schemaVersion,
        String owner,
        @JsonProperty("source_of_truth") String sourceOfTruth,
        @JsonProperty("run_id") String runId,
        @JsonProperty("ts") Object ts,
        @JsonProperty("workflow_id") String workflowId,
        String status,
        boolean ok,
        Map<String, Object> payload,
        Map<String, Object> config,
        Map<String, Object> result
) {
}
