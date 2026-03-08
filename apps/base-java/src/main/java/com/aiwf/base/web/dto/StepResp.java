package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record StepResp(
        @JsonProperty("job_id") String jobId,
        @JsonProperty("step_id") String stepId,
        String status,
        @JsonProperty("input_uri") String inputUri,
        @JsonProperty("output_uri") String outputUri,
        @JsonProperty("ruleset_version") String rulesetVersion,
        @JsonProperty("params_json") String paramsJson,
        @JsonProperty("started_at") Object startedAt,
        @JsonProperty("ended_at") Object endedAt,
        @JsonProperty("output_hash") String outputHash,
        String error
) {
}
