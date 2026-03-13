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
        assertThat(RuntimeTaskStatus.fromDb("failed")).isEqualTo(RuntimeTaskStatus.FAILED);
        assertThat(RuntimeTaskStatus.fromDb("cancelled")).isEqualTo(RuntimeTaskStatus.CANCELLED);
    }

    @Test
    void fromDbRejectsUnknownValue() {
        assertThatThrownBy(() -> RuntimeTaskStatus.fromDb("weird"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown runtime task status");
    }

    @Test
    void terminalAndCancelRulesAreExplicit() {
        assertThat(RuntimeTaskStatus.QUEUED.canCancel()).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.canCancel()).isTrue();
        assertThat(RuntimeTaskStatus.DONE.canCancel()).isFalse();
        assertThat(RuntimeTaskStatus.FAILED.canCancel()).isFalse();
        assertThat(RuntimeTaskStatus.CANCELLED.isTerminal()).isTrue();
        assertThat(RuntimeTaskStatus.FAILED.isTerminal()).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.isTerminal()).isFalse();
    }

    @Test
    void transitionRulesAreMonotonic() {
        assertThat(RuntimeTaskStatus.QUEUED.canTransitionTo(RuntimeTaskStatus.RUNNING)).isTrue();
        assertThat(RuntimeTaskStatus.QUEUED.canTransitionTo(RuntimeTaskStatus.DONE)).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.canTransitionTo(RuntimeTaskStatus.FAILED)).isTrue();
        assertThat(RuntimeTaskStatus.RUNNING.canTransitionTo(RuntimeTaskStatus.QUEUED)).isFalse();
        assertThat(RuntimeTaskStatus.DONE.canTransitionTo(RuntimeTaskStatus.FAILED)).isFalse();
        assertThat(RuntimeTaskStatus.CANCELLED.canTransitionTo(RuntimeTaskStatus.RUNNING)).isFalse();
    }
}
