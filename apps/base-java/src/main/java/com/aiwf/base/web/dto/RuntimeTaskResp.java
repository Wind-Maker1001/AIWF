package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RuntimeTaskResp(
        @JsonProperty("task_id") String taskId,
        @JsonProperty("tenant_id") String tenantId,
        String operator,
        String status,
        @JsonProperty("created_at_epoch") long createdAtEpoch,
        @JsonProperty("updated_at_epoch") long updatedAtEpoch,
        @JsonProperty("result_json") String resultJson,
        String error,
        String source,
        @JsonProperty("idempotency_key") String idempotencyKey,
        Integer attempts
) {
}
