package com.aiwf.base.db.model;

public record JobRow(
        String jobId,
        Object createdAt,
        String owner,
        JobStatus status
) {
}
