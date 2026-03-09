package com.aiwf.base.db.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StepStatusTest {

    @Test
    void fromDbMapsKnownValues() {
        assertThat(StepStatus.fromDb("RUNNING")).isEqualTo(StepStatus.RUNNING);
        assertThat(StepStatus.fromDb("done")).isEqualTo(StepStatus.DONE);
        assertThat(StepStatus.fromDb(" failed ")).isEqualTo(StepStatus.FAILED);
    }

    @Test
    void fromDbRejectsUnknownValue() {
        assertThatThrownBy(() -> StepStatus.fromDb("queued"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown step status");
    }

    @Test
    void transitionRulesAreExplicit() {
        assertThat(StepStatus.RUNNING.canTransitionTo(StepStatus.RUNNING)).isTrue();
        assertThat(StepStatus.RUNNING.canTransitionTo(StepStatus.DONE)).isTrue();
        assertThat(StepStatus.RUNNING.canTransitionTo(StepStatus.FAILED)).isTrue();
        assertThat(StepStatus.DONE.canTransitionTo(StepStatus.RUNNING)).isFalse();
        assertThat(StepStatus.FAILED.canTransitionTo(StepStatus.DONE)).isFalse();
        assertThat(StepStatus.DONE.isTerminal()).isTrue();
        assertThat(StepStatus.RUNNING.isTerminal()).isFalse();
    }
}
