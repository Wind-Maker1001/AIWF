package com.aiwf.base.db.model;

import java.util.Locale;

public enum StepStatus {
    RUNNING,
    DONE,
    FAILED;

    public String toDb() {
        return name();
    }

    public boolean isTerminal() {
        return this == DONE || this == FAILED;
    }

    public boolean canTransitionTo(StepStatus target) {
        if (target == null) {
            return false;
        }
        if (this == target) {
            return true;
        }
        return switch (this) {
            case RUNNING -> target == DONE || target == FAILED;
            case DONE, FAILED -> false;
        };
    }

    public static StepStatus fromDb(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("step status is blank");
        }
        try {
            return StepStatus.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown step status: " + value, e);
        }
    }
}
