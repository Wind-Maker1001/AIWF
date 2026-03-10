package com.aiwf.base.db.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobStatusTest {

    @Test
    void fromDbMapsKnownValues() {
        assertThat(JobStatus.fromDb("CREATED")).isEqualTo(JobStatus.CREATED);
        assertThat(JobStatus.fromDb("running")).isEqualTo(JobStatus.RUNNING);
        assertThat(JobStatus.fromDb(" done ")).isEqualTo(JobStatus.DONE);
        assertThat(JobStatus.fromDb("FAILED")).isEqualTo(JobStatus.FAILED);
    }

    @Test
    void fromDbRejectsUnknownValue() {
        assertThatThrownBy(() -> JobStatus.fromDb("queued"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown job status");
    }

    @Test
    void terminalStatesAreExplicit() {
        assertThat(JobStatus.DONE.isTerminal()).isTrue();
        assertThat(JobStatus.FAILED.isTerminal()).isTrue();
        assertThat(JobStatus.CREATED.isTerminal()).isFalse();
        assertThat(JobStatus.RUNNING.isTerminal()).isFalse();
    }
}
