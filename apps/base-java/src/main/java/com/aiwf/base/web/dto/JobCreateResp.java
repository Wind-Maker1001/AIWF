package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobCreateResp(
        @JsonProperty("job_id") String jobId,
        String owner,
        String status,
        @JsonProperty("job_root") String jobRoot,
        Map<String, Object> policy
) {
    public JobCreateResp withPolicy(Map<String, Object> newPolicy) {
        return new JobCreateResp(jobId, owner, status, jobRoot, newPolicy);
    }
}
