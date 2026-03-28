package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AuditEventResp(
        @JsonProperty("schema_version") String schemaVersion,
        String owner,
        @JsonProperty("source_of_truth") String sourceOfTruth,
        @JsonProperty("ts") Object ts,
        String actor,
        String action,
        @JsonProperty("job_id") String jobId,
        @JsonProperty("step_id") String stepId,
        Object detail
) {
}
