package com.aiwf.base.db.model;

public record RuntimeTaskRow(
        String taskId,
        String tenantId,
        String operator,
        RuntimeTaskStatus status,
        long createdAtEpoch,
        long updatedAtEpoch,
        String resultJson,
        String error,
        String source
) {
}
