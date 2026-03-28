package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record JobRunTimelineResp(
        @JsonProperty("schema_version") String schemaVersion,
        boolean ok,
        String owner,
        @JsonProperty("source_of_truth") String sourceOfTruth,
        @JsonProperty("run_id") String runId,
        String status,
        List<JobTimelineItemResp> timeline
) {
}
