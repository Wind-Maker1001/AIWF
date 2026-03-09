package com.aiwf.base.db.model;

import java.util.Locale;

public enum JobStatus {
    CREATED,
    RUNNING,
    DONE,
    FAILED;

    public String toDb() {
        return name();
    }

    public boolean isTerminal() {
        return this == DONE || this == FAILED;
    }

    public static JobStatus fromDb(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("job status is blank");
        }
        try {
            return JobStatus.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown job status: " + value, e);
        }
    }
}
