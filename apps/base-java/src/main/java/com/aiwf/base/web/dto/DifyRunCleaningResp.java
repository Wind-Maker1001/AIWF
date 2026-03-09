package com.aiwf.base.web.dto;

import com.aiwf.base.glue.GlueRunResult;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record DifyRunCleaningResp(
        boolean ok,
        @JsonProperty("job_id") String jobId,
        JobCreateResp job,
        GlueRunResult run,
        List<StepResp> steps,
        List<ArtifactResp> artifacts
) {
}
