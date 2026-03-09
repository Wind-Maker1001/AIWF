package com.aiwf.base.db.model;

public record JobRow(
        String jobId,
        Object createdAt,
        String owner,
        String status
) {
}
