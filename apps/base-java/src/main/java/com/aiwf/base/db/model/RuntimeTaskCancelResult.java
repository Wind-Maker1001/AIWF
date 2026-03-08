package com.aiwf.base.db.model;

public record RuntimeTaskCancelResult(
        boolean cancelled,
        RuntimeTaskRow task
) {
}
