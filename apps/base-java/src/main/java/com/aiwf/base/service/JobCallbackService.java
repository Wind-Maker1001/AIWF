package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.ArtifactRegisterReq;
import com.aiwf.base.web.dto.ArtifactRegisterResp;
import com.aiwf.base.web.dto.StepDoneCallbackReq;
import com.aiwf.base.web.dto.StepStartCallbackReq;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class JobCallbackService {

    private final JobRepository jobsRepo;
    private final JobStatusService jobStatus;

    public JobCallbackService(JobRepository jobsRepo, JobStatusService jobStatus) {
        this.jobsRepo = jobsRepo;
        this.jobStatus = jobStatus;
    }

    @Transactional
    public void stepStart(String jobId, String stepId, String actor, StepStartCallbackReq req) {
        requireJob(jobId);
        String paramsJson = JsonUtil.toJson(req.payload());
        StepTransitionResult result = jobsRepo.upsertStepRunning(
                jobId,
                stepId,
                req.getInputUri() == null ? "" : req.getInputUri(),
                req.getOutputUri() == null ? "" : req.getOutputUri(),
                req.getRulesetVersion(),
                paramsJson
        );
        StepRow step = requireStep(jobId, stepId, result.step());
        if (!"RUNNING".equals(step.status())) {
            throw stepConflict(jobId, stepId, "RUNNING", step.status());
        }

        jobStatus.onStepStart(jobId);
        jobsRepo.audit(new AuditEvent(jobId, actor, "STEP_START", stepId, paramsJson));
    }

    @Transactional
    public void stepDone(String jobId, String stepId, String actor, StepDoneCallbackReq req) {
        requireJob(jobId);
        String detailJson = JsonUtil.toJson(req.payload());
        StepTransitionResult result = jobsRepo.markStepDone(jobId, stepId, req.getOutputHash());
        StepRow step = requireStep(jobId, stepId, result.step());
        if (!"DONE".equals(step.status())) {
            throw stepConflict(jobId, stepId, "DONE", step.status());
        }

        if (result.changed()) {
            jobsRepo.audit(new AuditEvent(jobId, actor, "STEP_DONE", stepId, detailJson));
            jobStatus.onStepDone(jobId);
        }
    }

    @Transactional
    public ArtifactRegisterResp registerArtifact(String jobId, String actor, ArtifactRegisterReq body) {
        requireJob(jobId);
        jobsRepo.upsertArtifact(jobId, body.artifactId(), body.kind(), body.path(), body.sha256());
        jobsRepo.audit(new AuditEvent(jobId, actor, "ARTIFACT_REGISTER", null, JsonUtil.toJson(body.payload())));
        return new ArtifactRegisterResp(true, body.artifactId());
    }

    private JobRow requireJob(String jobId) {
        JobRow job = jobsRepo.getJob(jobId);
        if (job == null) {
            throw ApiException.notFound("job_not_found", "job not found", Map.of("job_id", jobId));
        }
        return job;
    }

    private StepRow requireStep(String jobId, String stepId, StepRow step) {
        if (step == null) {
            throw ApiException.notFound("step_not_found", "step not found", Map.of("job_id", jobId, "step_id", stepId));
        }
        return step;
    }

    private static ApiException stepConflict(String jobId, String stepId, String targetStatus, String currentStatus) {
        return ApiException.conflict(
                "step_transition_conflict",
                "step cannot transition to " + targetStatus,
                Map.of("job_id", jobId, "step_id", stepId, "current_status", currentStatus)
        );
    }
}
