package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.JobStatus;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepStatus;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.glue.GlueRunFlowReq;
import com.aiwf.base.glue.GlueGateway;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.StepFailReq;
import com.aiwf.base.web.dto.StepFailResp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
    void setUp() throws Exception {
        service = new JobService(jobs, jobStatus, glue);
        Path busRoot = Files.createTempDirectory("aiwf-jobservice-test");
        ReflectionTestUtils.setField(service, "jobsBusRoot", busRoot.toString());
    }

    @Test
    void manualStepFailAlsoUpdatesJobStatus() {
        when(jobs.getJob("job1")).thenReturn(new JobRow("job1", null, "owner", JobStatus.RUNNING));
        when(jobs.markStepFailed("job1", "step1", "boom"))
                .thenReturn(new StepTransitionResult(
                        new StepRow("job1", "step1", StepStatus.FAILED, null, null, "v1", "{}", null, null, null, "boom"),
                        true
                ));

        StepFailResp out = service.stepFail("job1", "step1", "manual", new StepFailReq(null, "boom"));

        assertThat(out.ok()).isTrue();
        assertThat(out.jobId()).isEqualTo("job1");
        assertThat(out.stepId()).isEqualTo("step1");
        verify(jobStatus).onStepFail("job1");
        verify(jobs).audit(new AuditEvent("job1", "manual", "STEP_FAIL", "step1", "boom"));
    }

    @Test
    void manualStepFailRejectsUnknownStep() {
        when(jobs.getJob("job1")).thenReturn(new JobRow("job1", null, "owner", JobStatus.RUNNING));
        when(jobs.markStepFailed("job1", "missing", "boom"))
                .thenReturn(new StepTransitionResult(null, false));

        assertThatThrownBy(() -> service.stepFail("job1", "missing", "manual", new StepFailReq(null, "boom")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("step not found");

        verify(jobStatus, never()).onStepFail("job1");
    }

    @Test
    void manualStepFailRejectsDoneStep() {
        when(jobs.getJob("job1")).thenReturn(new JobRow("job1", null, "owner", JobStatus.RUNNING));
        when(jobs.markStepFailed("job1", "step1", "boom"))
                .thenReturn(new StepTransitionResult(
                        new StepRow("job1", "step1", StepStatus.DONE, null, null, "v1", "{}", null, null, "abc123", null),
                        false
                ));

        assertThatThrownBy(() -> service.stepFail("job1", "step1", "manual", new StepFailReq(null, "boom")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("cannot transition to FAILED");

        verify(jobStatus, never()).onStepFail("job1");
        verify(jobs, never()).audit(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void manualStepFailIsIdempotentWhenAlreadyFailed() {
        when(jobs.getJob("job1")).thenReturn(new JobRow("job1", null, "owner", JobStatus.RUNNING));
        when(jobs.markStepFailed("job1", "step1", "boom"))
                .thenReturn(new StepTransitionResult(
                        new StepRow("job1", "step1", StepStatus.FAILED, null, null, "v1", "{}", null, null, null, "boom"),
                        false
                ));

        StepFailResp out = service.stepFail("job1", "step1", "manual", new StepFailReq(null, "boom"));

        assertThat(out.ok()).isTrue();
        verify(jobStatus, never()).onStepFail("job1");
        verify(jobs, never()).audit(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void manualStepFailRejectsUnknownJob() {
        when(jobs.getJob("missing-job")).thenReturn(null);

        assertThatThrownBy(() -> service.stepFail("missing-job", "step1", "manual", new StepFailReq(null, "boom")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("job not found");

        verifyNoInteractions(jobStatus);
        verify(glue, never()).health();
    }

    @Test
    void listStepsRejectsUnknownJob() {
        when(jobs.getJob("missing-job")).thenReturn(null);

        assertThatThrownBy(() -> service.listSteps("missing-job"))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("job not found");
    }

    @Test
    void runFlowRejectsUnknownJob() {
        when(jobs.getJob("missing")).thenReturn(null);

        assertThatThrownBy(() -> service.runFlow("missing", "cleaning", "ops", "v1", Map.of("rows", 1)))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("job not found");

        verifyNoInteractions(glue);
    }

    @Test
    void runFlowBuildsExplicitJobContextWhileKeepingJobRootFallback() {
        when(jobs.getJob("job1")).thenReturn(new JobRow("job1", null, "owner", "running"));
        when(glue.runFlow(org.mockito.ArgumentMatchers.eq("job1"), org.mockito.ArgumentMatchers.eq("cleaning"), any()))
                .thenReturn(GlueRunResult.fromMap(Map.of("ok", true, "job_id", "job1", "flow", "cleaning"), "job1", "cleaning"));

        @SuppressWarnings("unchecked")
        org.mockito.ArgumentCaptor<GlueRunFlowReq> reqCap = org.mockito.ArgumentCaptor.forClass(GlueRunFlowReq.class);

        GlueRunResult out = service.runFlow("job1", "cleaning", "ops", "v2", Map.of(
                "sample", true,
                "job_root", "D:\\legacy\\job1",
                "stage_dir", "D:\\legacy\\job1\\stage",
                "artifacts_dir", "D:\\legacy\\job1\\artifacts",
                "evidence_dir", "D:\\legacy\\job1\\evidence",
                "job_context", Map.of("job_root", "D:\\legacy\\nested"),
                "trace_id", "legacy-trace"
        ));

        assertThat(out.isOk()).isTrue();
        verify(glue).runFlow(org.mockito.ArgumentMatchers.eq("job1"), org.mockito.ArgumentMatchers.eq("cleaning"), reqCap.capture());

        GlueRunFlowReq req = reqCap.getValue();
        assertThat(req.jobId()).isEqualTo("job1");
        assertThat(req.flow()).isEqualTo("cleaning");
        assertThat(req.actor()).isEqualTo("ops");
        assertThat(req.rulesetVersion()).isEqualTo("v2");
        assertThat(req.traceId()).isNotBlank();
        assertThat(req.jobContext()).isNotNull();
        assertThat(req.jobContext().jobRoot()).endsWith(Path.of("jobs", "job1").toString());
        assertThat(req.jobContext().stageDir()).endsWith(Path.of("jobs", "job1", "stage").toString());
        assertThat(req.jobContext().artifactsDir()).endsWith(Path.of("jobs", "job1", "artifacts").toString());
        assertThat(req.jobContext().evidenceDir()).endsWith(Path.of("jobs", "job1", "evidence").toString());
        assertThat(req.params()).containsEntry("sample", true);
        assertThat(req.params()).containsEntry("job_root", "D:\\legacy\\job1");
        assertThat(req.params()).doesNotContainKeys(
                "stage_dir",
                "artifacts_dir",
                "evidence_dir",
                "job_context",
                "trace_id"
        );
    }
}
