package com.aiwf.base.db.model;

public record StepTransitionResult(
        StepRow step,
        boolean changed
) {
}
