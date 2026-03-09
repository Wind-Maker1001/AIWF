package com.aiwf.base.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record RuntimeTaskCancelResp(
        boolean ok,
        @JsonProperty("task_id") String taskId,
        boolean cancelled,
        String status
) {
}
