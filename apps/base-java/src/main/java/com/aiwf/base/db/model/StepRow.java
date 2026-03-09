package com.aiwf.base.db.model;

public record StepRow(
        String jobId,
        String stepId,
        String status,
        String inputUri,
        String outputUri,
        String rulesetVersion,
        String paramsJson,
        Object startedAt,
        Object endedAt,
        String outputHash,
        String error
) {
}
