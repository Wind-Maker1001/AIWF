package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RuntimeTaskUpsertReq(
        @JsonProperty("task_id") String taskId,
        @JsonProperty("operator") String operator,
        @JsonProperty("status") String status,
        @JsonProperty("created_at") Long createdAt,
        @JsonProperty("updated_at") Long updatedAt,
        @JsonProperty("result") Object result,
        @JsonProperty("error") String error,
        @JsonProperty("source") String source,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("idempotency_key") String idempotencyKey,
        @JsonProperty("attempts") Integer attempts
) {
}
