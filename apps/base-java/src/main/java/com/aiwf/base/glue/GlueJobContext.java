package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record GlueJobContext(
        @JsonProperty("job_root") String jobRoot,
        @JsonProperty("stage_dir") String stageDir,
        @JsonProperty("artifacts_dir") String artifactsDir,
        @JsonProperty("evidence_dir") String evidenceDir
) {
}
