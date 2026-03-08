package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.glue.GlueGateway;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.StepFailReq;
import com.aiwf.base.web.dto.StepFailResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobs;

    @Mock
    private JobStatusService jobStatus;

    @Mock
    private GlueGateway glue;

    private JobService service;

    @BeforeEach
    void setUp() {
        service = new JobService(jobs, jobStatus, glue);
    }

    @Test
    void manualStepFailAlsoUpdatesJobStatus() {
        when(jobs.markStepFailed("job1", "step1", "boom")).thenReturn(1);

        StepFailResp out = service.stepFail("job1", "step1", "manual", new StepFailReq(null, "boom"));

        assertThat(out.ok()).isTrue();
        assertThat(out.jobId()).isEqualTo("job1");
        assertThat(out.stepId()).isEqualTo("step1");
        verify(jobStatus).onStepFail("job1");
        verify(jobs).audit(new AuditEvent("job1", "manual", "STEP_FAIL", "step1", "boom"));
    }

    @Test
    void manualStepFailRejectsUnknownStep() {
        when(jobs.markStepFailed("job1", "missing", "boom")).thenReturn(0);

        assertThatThrownBy(() -> service.stepFail("job1", "missing", "manual", new StepFailReq(null, "boom")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("step not found");

        verify(jobStatus, never()).onStepFail("job1");
    }

    @Test
    void runFlowRejectsUnknownJob() {
        when(jobs.getJob("missing")).thenReturn(null);

        assertThatThrownBy(() -> service.runFlow("missing", "cleaning", "ops", "v1", Map.of("rows", 1)))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("job not found");

        verifyNoInteractions(glue);
    }
}
