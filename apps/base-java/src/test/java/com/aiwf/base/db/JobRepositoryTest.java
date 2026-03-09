package com.aiwf.base.db;

import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepTransitionResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobRepositoryTest {

    @Mock
    private JdbcTemplate jdbc;

    private JobRepository repository;

    @BeforeEach
    void setUp() {
        repository = new JobRepository(jdbc);
    }

    @Test
    void upsertStepRunningRetriesUpdateAfterDuplicateInsert() {
        when(jdbc.update(startsWith("UPDATE dbo.steps"), any(), any(), any(), any(), any(), any()))
                .thenReturn(0, 1);
        when(jdbc.update(startsWith("INSERT INTO dbo.steps"), any(), any(), any(), any(), any(), any()))
                .thenThrow(new DuplicateKeyException("duplicate"));
        when(jdbc.queryForObject(
                startsWith("SELECT job_id, step_id, status"),
                org.mockito.ArgumentMatchers.<RowMapper<StepRow>>any(),
                eq("job1"),
                eq("step1")
        )).thenReturn(new StepRow("job1", "step1", "RUNNING", "in", "out", "v1", "{}", null, null, null, null));

        StepTransitionResult result = repository.upsertStepRunning("job1", "step1", "in", "out", "v1", "{}");

        verify(jdbc, times(2)).update(startsWith("UPDATE dbo.steps"), any(), any(), any(), any(), any(), any());
        verify(jdbc).update(startsWith("INSERT INTO dbo.steps"), any(), any(), any(), any(), any(), any());
        assertThat(result.changed()).isTrue();
        assertThat(result.step()).isNotNull();
        assertThat(result.step().status()).isEqualTo("RUNNING");
    }

    @Test
    void upsertStepRunningDoesNotReopenDoneStep() {
        when(jdbc.update(startsWith("UPDATE dbo.steps"), any(), any(), any(), any(), any(), any()))
                .thenReturn(0, 0);
        when(jdbc.update(startsWith("INSERT INTO dbo.steps"), any(), any(), any(), any(), any(), any()))
                .thenThrow(new DuplicateKeyException("duplicate"));
        when(jdbc.queryForObject(
                startsWith("SELECT job_id, step_id, status"),
                org.mockito.ArgumentMatchers.<RowMapper<StepRow>>any(),
                eq("job1"),
                eq("step1")
        )).thenReturn(new StepRow("job1", "step1", "DONE", "in", "out", "v1", "{}", null, null, "abc123", null));

        StepTransitionResult result = repository.upsertStepRunning("job1", "step1", "in", "out", "v1", "{}");

        assertThat(result.changed()).isFalse();
        assertThat(result.step()).isNotNull();
        assertThat(result.step().status()).isEqualTo("DONE");
    }

    @Test
    void auditWritesDetailJson() {
        repository.audit(new AuditEvent("job1", "actor1", "STEP_START", "step1", "{\"ok\":true}"));

        verify(jdbc).update(
                startsWith("INSERT INTO dbo.audit_log"),
                eq("job1"),
                eq("actor1"),
                eq("STEP_START"),
                eq("step1"),
                eq("{\"ok\":true}")
        );
    }
}
