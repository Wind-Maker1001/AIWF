package com.aiwf.base.web;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.service.JobService;
import com.aiwf.base.service.JobStatusService;
import com.aiwf.base.service.JsonUtil;
import com.aiwf.base.web.dto.ArtifactRegisterReq;
import com.aiwf.base.web.dto.ArtifactRegisterResp;
import com.aiwf.base.web.dto.OkResp;
import com.aiwf.base.web.dto.StepDoneCallbackReq;
import com.aiwf.base.web.dto.StepFailCallbackReq;
import com.aiwf.base.web.dto.StepFailResp;
import com.aiwf.base.web.dto.StepStartCallbackReq;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class CallbackController {

    private final JobRepository jobsRepo;
    private final JobStatusService jobStatus;
    private final JobService jobs;

    public CallbackController(JobRepository jobsRepo, JobStatusService jobStatus, JobService jobs) {
        this.jobsRepo = jobsRepo;
        this.jobStatus = jobStatus;
        this.jobs = jobs;
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/start", consumes = {"application/json", "*/*"})
    public OkResp stepStart(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepStartCallbackReq body
    ) {
        StepStartCallbackReq req = body == null ? new StepStartCallbackReq() : body;
        String paramsJson = JsonUtil.toJson(req.payload());
        StepTransitionResult result = jobsRepo.upsertStepRunning(
                jobId,
                stepId,
                req.getInputUri() == null ? "" : req.getInputUri(),
                req.getOutputUri() == null ? "" : req.getOutputUri(),
                req.getRulesetVersion(),
                paramsJson
        );
        StepRow step = result.step();
        if (step == null) {
            throw ApiException.notFound("step_not_found", "step not found", Map.of("job_id", jobId, "step_id", stepId));
        }
        if (!"RUNNING".equals(step.status())) {
            throw stepConflict(jobId, stepId, "RUNNING", step.status());
        }
        jobStatus.onStepStart(jobId);
        jobsRepo.audit(new AuditEvent(jobId, actor, "STEP_START", stepId, paramsJson));
        return OkResp.success();
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/done", consumes = {"application/json", "*/*"})
    public OkResp stepDone(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepDoneCallbackReq body
    ) {
        StepDoneCallbackReq req = body == null ? new StepDoneCallbackReq() : body;
        String detailJson = JsonUtil.toJson(req.payload());

        StepTransitionResult result = jobsRepo.markStepDone(jobId, stepId, req.getOutputHash());
        StepRow step = result.step();
        if (step == null) {
            throw ApiException.notFound("step_not_found", "step not found", Map.of("job_id", jobId, "step_id", stepId));
        }
        if (!"DONE".equals(step.status())) {
            throw stepConflict(jobId, stepId, "DONE", step.status());
        }
        if (result.changed()) {
            jobsRepo.audit(new AuditEvent(jobId, actor, "STEP_DONE", stepId, detailJson));
            jobStatus.onStepDone(jobId);
        }

        return OkResp.success();
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/fail", consumes = {"application/json", "*/*"})
    public StepFailResp stepFail(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepFailCallbackReq body
    ) {
        StepFailCallbackReq req = body == null ? new StepFailCallbackReq() : body;
        String error = req.errorOrDefault("failed");
        return jobs.failStep(jobId, stepId, actor, error, JsonUtil.toJson(req.payload(error)));
    }

    @PostMapping(value = "/jobs/{jobId}/artifacts/register", consumes = {"application/json", "*/*"})
    public ArtifactRegisterResp registerArtifact(
            @PathVariable @NotBlank String jobId,
            @RequestParam(defaultValue = "glue") String actor,
            @Valid @RequestBody ArtifactRegisterReq body
    ) {
        jobsRepo.upsertArtifact(jobId, body.artifactId(), body.kind(), body.path(), body.sha256());
        jobsRepo.audit(new AuditEvent(jobId, actor, "ARTIFACT_REGISTER", null, JsonUtil.toJson(body.payload())));
        return new ArtifactRegisterResp(true, body.artifactId());
    }

    private static ApiException stepConflict(String jobId, String stepId, String targetStatus, String currentStatus) {
        return ApiException.conflict(
                "step_transition_conflict",
                "step cannot transition to " + targetStatus,
                Map.of("job_id", jobId, "step_id", stepId, "current_status", currentStatus)
        );
    }
}
