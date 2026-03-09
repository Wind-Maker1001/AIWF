package com.aiwf.base.db.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuntimeTaskStatusTest {

    @Test
    void fromDbMapsKnownValues() {
        assertThat(RuntimeTaskStatus.fromDb("queued")).isEqualTo(RuntimeTaskStatus.QUEUED);
        assertThat(RuntimeTaskStatus.fromDb("RUNNING")).isEqualTo(RuntimeTaskStatus.RUNNING);
        assertThat(RuntimeTaskStatus.fromDb(" done ")).isEqualTo(RuntimeTaskStatus.DONE);
        assertThat(RuntimeTaskStatus.fromDb("cancelled")).isEqualTo(RuntimeTaskStatus.CANCELLED);
    }

    @Test
    void fromDbRejectsUnknownValue() {
        assertThatThrownBy(() -> RuntimeTaskStatus.fromDb("failed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown runtime task status");
    }

    @Test
    void terminalAndCancelRulesAreExplicit() {
        assertThat(RuntimeTaskStatus.QUEUED.canCancel()).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.canCancel()).isTrue();
        assertThat(RuntimeTaskStatus.DONE.canCancel()).isFalse();
        assertThat(RuntimeTaskStatus.CANCELLED.isTerminal()).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.isTerminal()).isFalse();
    }
}
