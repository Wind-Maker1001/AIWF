package com.aiwf.base.db.model;

public record AuditEvent(
        String jobId,
        String actor,
        String action,
        String stepId,
        String detailJson
) {
}
