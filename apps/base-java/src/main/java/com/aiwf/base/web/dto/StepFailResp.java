package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record StepFailResp(
        boolean ok,
        @JsonProperty("job_id") String jobId,
        @JsonProperty("step_id") String stepId
) {
}
