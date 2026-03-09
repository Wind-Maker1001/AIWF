package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RuntimeTaskUpsertResp(
        boolean ok,
        @JsonProperty("task_id") String taskId,
        @JsonProperty("tenant_id") String tenantId,
        String status
) {
}
