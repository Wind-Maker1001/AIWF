package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record JobDetailsResp(
        @JsonProperty("job_id") String jobId,
        String owner,
        String status,
        @JsonProperty("created_at") Object createdAt
) {
}
