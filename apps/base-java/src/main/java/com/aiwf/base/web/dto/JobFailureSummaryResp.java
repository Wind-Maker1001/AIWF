package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public record JobFailureSummaryResp(
        @JsonProperty("schema_version") String schemaVersion,
        boolean ok,
        String owner,
        @JsonProperty("source_of_truth") String sourceOfTruth,
        @JsonProperty("total_runs") int totalRuns,
        @JsonProperty("failed_runs") int failedRuns,
        @JsonProperty("by_node") Map<String, Map<String, Object>> byNode
) {
}
