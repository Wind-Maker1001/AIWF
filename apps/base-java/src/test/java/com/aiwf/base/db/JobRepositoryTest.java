package com.aiwf.base.db;

import com.aiwf.base.db.model.AuditEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

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

        repository.upsertStepRunning("job1", "step1", "in", "out", "v1", "{}");

        verify(jdbc, times(2)).update(startsWith("UPDATE dbo.steps"), any(), any(), any(), any(), any(), any());
        verify(jdbc).update(startsWith("INSERT INTO dbo.steps"), any(), any(), any(), any(), any(), any());
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
