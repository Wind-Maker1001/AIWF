package com.aiwf.base.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class JobStatusServiceTest {

    @Mock
    private JdbcTemplate jdbc;

    private JobStatusService service;

    @BeforeEach
    void setUp() {
        service = new JobStatusService(jdbc);
    }

    @Test
    void onStepStartMovesDoneJobsBackToRunning() {
        service.onStepStart("job1");

        verify(jdbc).update(
                argThat(sql -> sql.contains("status <> ?")),
                eq("RUNNING"),
                eq("job1")
                ,
                eq("FAILED")
        );
    }
}
