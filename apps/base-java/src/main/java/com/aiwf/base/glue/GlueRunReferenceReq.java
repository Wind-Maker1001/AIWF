package com.aiwf.base.glue;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record GlueRunReferenceReq(
        @JsonProperty("version_id") String versionId,
        @JsonProperty("published_version_id") String publishedVersionId,
        String actor,
        @JsonProperty("ruleset_version") String rulesetVersion,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("job_context") GlueJobContext jobContext,
        Map<String, Object> params
) {
}
