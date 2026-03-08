package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record RuntimeTaskListResp(
        boolean ok,
        @JsonProperty("tenant_id") String tenantId,
        List<RuntimeTaskResp> tasks
) {
}
