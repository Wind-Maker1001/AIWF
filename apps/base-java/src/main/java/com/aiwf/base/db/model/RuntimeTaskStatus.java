package com.aiwf.base.db.model;

import java.util.Locale;

public enum RuntimeTaskStatus {
    QUEUED,
    RUNNING,
    DONE,
    FAILED,
    CANCELLED;

    public String toDb() {
        return name().toLowerCase(Locale.ROOT);
    }

    public boolean isTerminal() {
        return this == DONE || this == FAILED || this == CANCELLED;
    }

    public boolean canCancel() {
        return this == QUEUED || this == RUNNING;
    }

    public boolean canTransitionTo(RuntimeTaskStatus next) {
        if (next == null) {
            return false;
        }
        if (this == next) {
            return true;
        }
        if (isTerminal()) {
            return false;
        }
        if (this == QUEUED) {
            return next == RUNNING || next.isTerminal();
        }
        if (this == RUNNING) {
            return next.isTerminal();
        }
        return false;
    }

    public static RuntimeTaskStatus fromDb(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("runtime task status is blank");
        }
        try {
            return RuntimeTaskStatus.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown runtime task status: " + value, e);
        }
    }
}
