package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.ArtifactRow;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.JobStatus;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepStatus;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.glue.GlueGateway;
import com.aiwf.base.glue.GlueHealthResult;
import com.aiwf.base.glue.GlueJobContext;
import com.aiwf.base.glue.GlueRunFlowReq;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.ArtifactResp;
import com.aiwf.base.web.dto.JobCreateResp;
import com.aiwf.base.web.dto.JobDetailsResp;
import com.aiwf.base.web.dto.StepFailReq;
import com.aiwf.base.web.dto.StepFailResp;
import com.aiwf.base.web.dto.StepResp;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class JobService {
    private static final String JOB_DIRS_FAIL_ACTION = "JOB_DIRS_FAIL";
    private static final Set<String> RESERVED_GLUE_PARAM_KEYS = Set.of(
            "job_id",
            "flow",
            "actor",
            "ruleset_version",
            "trace_id",
            "job_context",
            "stage_dir",
            "artifacts_dir",
            "evidence_dir"
    );

    private final JobRepository jobs;
    private final JobStatusService jobStatus;
    private final GlueGateway glue;

    @Value("${aiwf.bus:R:\\aiwf}")
    private String jobsBusRoot;

    public JobService(JobRepository jobs, JobStatusService jobStatus, GlueGateway glue) {
        this.jobs = jobs;
        this.jobStatus = jobStatus;
        this.glue = glue;
    }

    public GlueHealthResult glueHealth() {
        try {
            return glue.health();
        } catch (RestClientException e) {
            return GlueHealthResult.unavailable(e.getMessage());
        }
    }

    public JobCreateResp createJob(String owner) {
        String jobId = jobs.createJob(owner);
        try {
            ensureJobDirs(jobId);
        } catch (RuntimeException e) {
            recordJobDirFailure(jobId, defaultIfBlank(owner, "base"), e);
            throw ApiException.internalServerError(
                    "job_dirs_failed",
                    "failed to prepare job directories",
                    Map.of("job_id", jobId)
            );
        }
        return new JobCreateResp(jobId, owner, JobStatus.RUNNING.toDb(), Paths.get(jobsRoot(), jobId).toString(), null);
    }

    public JobCreateResp createJob(String owner, Map<String, Object> policy) {
        JobCreateResp out = createJob(owner);
        if (policy != null && !policy.isEmpty()) {
            return out.withPolicy(Map.copyOf(policy));
        }
        return out;
    }

    public JobDetailsResp getJob(String jobId) {
        return toJobDetails(requireJob(jobId));
    }

    public List<StepResp> listSteps(String jobId) {
        requireJob(jobId);
        return jobs.listSteps(jobId).stream().map(this::toStepResp).toList();
    }

    public List<ArtifactResp> listArtifacts(String jobId) {
        requireJob(jobId);
        return jobs.listArtifacts(jobId).stream().map(this::toArtifactResp).toList();
    }

    public GlueRunResult runFlow(
            String jobId,
            String flow,
            String actor,
            String rulesetVersion,
            Map<String, Object> params
    ) {
        if (jobs.getJob(jobId) == null) {
            throw ApiException.notFound("job_not_found", "job not found", Map.of("job_id", jobId));
        }

        String effectiveActor = defaultIfBlank(actor, "glue");
        String effectiveRuleset = (rulesetVersion == null || rulesetVersion.isBlank()) ? "v1" : rulesetVersion;
        try {
            ensureJobDirs(jobId);
        } catch (RuntimeException e) {
            recordJobDirFailure(jobId, effectiveActor, e);
            return GlueRunResult.failed(jobId, flow, "failed to prepare job directories");
        }

        try {
            GlueJobContext jobContext = buildJobContext(jobId);
            return glue.runFlow(jobId, flow, new GlueRunFlowReq(
                    jobId,
                    flow,
                    effectiveActor,
                    effectiveRuleset,
                    newTraceId(),
                    jobContext,
                    buildGlueParams(params)
            ));
        } catch (RestClientException e) {
            jobs.audit(new AuditEvent(jobId, effectiveActor, "FLOW_RUN_FAIL", flow, e.getMessage()));
            jobStatus.onStepFail(jobId);
            return GlueRunResult.failed(jobId, flow, e.getMessage());
        }
    }

    public GlueRunResult runFlow(
            String jobId,
            String flow,
            String actor,
            Map<String, Object> params
    ) {
        return runFlow(jobId, flow, actor, "v1", params);
    }

    @Transactional
    public StepFailResp failStep(String jobId, String stepId, String actor, String error, String auditDetail) {
        requireJob(jobId);
        String effectiveActor = defaultIfBlank(actor, "manual");
        String effectiveError = defaultIfBlank(error, "manual stepFail");
        StepTransitionResult result = jobs.markStepFailed(jobId, stepId, effectiveError);
        StepRow step = result.step();
        if (step == null) {
            throw ApiException.notFound("step_not_found", "step not found", Map.of("job_id", jobId, "step_id", stepId));
        }
        if (step.status() != StepStatus.FAILED) {
            throw ApiException.conflict(
                    "step_transition_conflict",
                    "step cannot transition to " + StepStatus.FAILED.toDb(),
                    Map.of("job_id", jobId, "step_id", stepId, "current_status", step.status().toDb())
            );
        }

        if (result.changed()) {
            jobs.audit(new AuditEvent(jobId, effectiveActor, "STEP_FAIL", stepId, defaultIfBlank(auditDetail, effectiveError)));
            jobStatus.onStepFail(jobId);
        }
        return new StepFailResp(true, jobId, stepId);
    }

    public StepFailResp stepFail(String jobId, String stepId, String actor, StepFailReq req) {
        String msg = req == null ? "manual stepFail" : defaultIfBlank(req.error(), "manual stepFail");
        return failStep(jobId, stepId, actor, msg, msg);
    }

    private void ensureJobDirs(String jobId) {
        Path jobRoot = Paths.get(jobsRoot(), jobId);
        try {
            Files.createDirectories(jobRoot);
            Files.createDirectories(jobRoot.resolve("stage"));
            Files.createDirectories(jobRoot.resolve("artifacts"));
            Files.createDirectories(jobRoot.resolve("evidence"));
        } catch (Exception e) {
            throw new IllegalStateException("failed to prepare job directories for " + jobId, e);
        }
    }

    private void recordJobDirFailure(String jobId, String actor, Exception error) {
        jobs.audit(new AuditEvent(jobId, actor, JOB_DIRS_FAIL_ACTION, null, defaultIfBlank(error.getMessage(), error.getClass().getSimpleName())));
        jobStatus.onStepFail(jobId);
    }

    private String jobsRoot() {
        return Paths.get(jobsBusRoot, "jobs").toString();
    }

    private GlueJobContext buildJobContext(String jobId) {
        Path jobRoot = Paths.get(jobsRoot(), jobId);
        return new GlueJobContext(
                jobRoot.toString(),
                jobRoot.resolve("stage").toString(),
                jobRoot.resolve("artifacts").toString(),
                jobRoot.resolve("evidence").toString()
        );
    }

    private Map<String, Object> buildGlueParams(Map<String, Object> params) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (params == null || params.isEmpty()) {
            return out;
        }
        // Keep the Glue transport contract explicit, but preserve job_root until master-side glue consumes only job_context.
        params.forEach((key, value) -> {
            if (!RESERVED_GLUE_PARAM_KEYS.contains(key)) {
                out.put(key, value);
            }
        });
        return out;
    }

    private String newTraceId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private JobRow requireJob(String jobId) {
        JobRow job = jobs.getJob(jobId);
        if (job == null) {
            throw ApiException.notFound("job_not_found", "job not found", Map.of("job_id", jobId));
        }
        return job;
    }

    private JobDetailsResp toJobDetails(JobRow row) {
        return new JobDetailsResp(row.jobId(), row.owner(), row.status().toDb(), row.createdAt());
    }

    private StepResp toStepResp(StepRow row) {
        return new StepResp(
                row.jobId(),
                row.stepId(),
                row.status().toDb(),
                row.inputUri(),
                row.outputUri(),
                row.rulesetVersion(),
                row.paramsJson(),
                row.startedAt(),
                row.endedAt(),
                row.outputHash(),
                row.error()
        );
    }

    private ArtifactResp toArtifactResp(ArtifactRow row) {
        return new ArtifactResp(row.artifactId(), row.kind(), row.path(), row.sha256(), row.createdAt());
    }

    private static String defaultIfBlank(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }
}
