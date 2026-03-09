package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.ArtifactRegisterReq;
import com.aiwf.base.web.dto.ArtifactRegisterResp;
import com.aiwf.base.web.dto.StepDoneCallbackReq;
import com.aiwf.base.web.dto.StepStartCallbackReq;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobCallbackServiceTest {

    @Mock
    private JobRepository jobsRepo;

    @Mock
    private JobStatusService jobStatus;

    private JobCallbackService service;

    @BeforeEach
    void setUp() {
        service = new JobCallbackService(jobsRepo, jobStatus);
    }

    @Test
    void stepStartAuditsAndUpdatesStatus() {
        StepStartCallbackReq req = new StepStartCallbackReq();
        req.setInputUri("bus://input.csv");
        req.setOutputUri("bus://output.csv");
        req.setRulesetVersion("v3");
        when(jobsRepo.upsertStepRunning(eq("job1"), eq("cleaning"), eq("bus://input.csv"), eq("bus://output.csv"), eq("v3"), any()))
                .thenReturn(new StepTransitionResult(
                        new StepRow("job1", "cleaning", "RUNNING", "bus://input.csv", "bus://output.csv", "v3", "{}", null, null, null, null),
                        true
                ));

        service.stepStart("job1", "cleaning", "glue", req);

        verify(jobStatus).onStepStart("job1");
        verify(jobsRepo).audit(any(AuditEvent.class));
    }

    @Test
    void stepDoneConflictDoesNotAudit() {
        StepDoneCallbackReq req = new StepDoneCallbackReq();
        when(jobsRepo.markStepDone(eq("job1"), eq("cleaning"), eq(null)))
                .thenReturn(new StepTransitionResult(
                        new StepRow("job1", "cleaning", "FAILED", null, null, "v1", "{}", null, null, null, "boom"),
                        false
                ));

        assertThatThrownBy(() -> service.stepDone("job1", "cleaning", "glue", req))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("cannot transition to DONE");

        verify(jobsRepo, never()).audit(any());
        verify(jobStatus, never()).onStepDone(any());
    }

    @Test
    void registerArtifactAuditsAndReturnsResponse() {
        ArtifactRegisterReq req = new ArtifactRegisterReq("art1", "xlsx", "D:\\AIWF\\bus\\jobs\\j1\\artifacts\\a.xlsx", "sha", null);

        ArtifactRegisterResp resp = service.registerArtifact("job1", "glue", req);

        assertThat(resp.ok()).isTrue();
        assertThat(resp.artifactId()).isEqualTo("art1");
        verify(jobsRepo).upsertArtifact("job1", "art1", "xlsx", "D:\\AIWF\\bus\\jobs\\j1\\artifacts\\a.xlsx", "sha");
        verify(jobsRepo).audit(any(AuditEvent.class));
    }
}
