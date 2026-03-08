package com.aiwf.base.db;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RuntimeTaskRepositoryTest {

    @Mock
    private JdbcTemplate jdbc;

    private RuntimeTaskRepository repository;

    @BeforeEach
    void setUp() {
        repository = new RuntimeTaskRepository(jdbc);
    }

    @Test
    void upsertTaskDoesNotOverwriteCreatedAtOnUpdate() {
        when(jdbc.update(startsWith("UPDATE dbo.workflow_tasks"), any(), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(1);

        repository.upsertTask("task1", "tenant-a", "transform_rows_v2", "done", 111L, 222L, "{\"rows\":1}", null, "accel-rust");

        verify(jdbc).update(
                startsWith("UPDATE dbo.workflow_tasks"),
                eq("tenant-a"),
                eq("transform_rows_v2"),
                eq("done"),
                eq(222L),
                eq("{\"rows\":1}"),
                isNull(),
                eq("accel-rust"),
                eq("task1")
        );
    }
}
