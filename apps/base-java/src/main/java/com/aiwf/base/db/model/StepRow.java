package com.aiwf.base.db.model;

public record StepRow(
        String jobId,
        String stepId,
        StepStatus status,
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
