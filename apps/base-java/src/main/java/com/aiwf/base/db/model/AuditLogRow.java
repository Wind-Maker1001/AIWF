package com.aiwf.base.db.model;

public record AuditLogRow(
        Object createdAt,
        String actor,
        String action,
        String jobId,
        String stepId,
        String detailJson
) {
}
