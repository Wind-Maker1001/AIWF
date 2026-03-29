package com.aiwf.base.service;

import com.aiwf.base.db.JobRepository;
import com.aiwf.base.db.model.ArtifactRow;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.AuditLogRow;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.JobStatus;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepStatus;
import com.aiwf.base.db.model.StepTransitionResult;
import com.aiwf.base.glue.GlueGateway;
import com.aiwf.base.glue.GlueHealthResult;
import com.aiwf.base.glue.GlueJobContext;
import com.aiwf.base.glue.GlueRunFlowReq;
import com.aiwf.base.glue.GlueRunReferenceReq;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.AuditEventResp;
import com.aiwf.base.web.dto.ArtifactResp;
import com.aiwf.base.web.dto.JobFailureSummaryResp;
import com.aiwf.base.web.dto.JobCreateResp;
import com.aiwf.base.web.dto.JobDetailsResp;
import com.aiwf.base.web.dto.JobRunRecordResp;
import com.aiwf.base.web.dto.JobRunTimelineResp;
import com.aiwf.base.web.dto.JobTimelineItemResp;
import com.aiwf.base.web.dto.StepFailReq;
import com.aiwf.base.web.dto.StepFailResp;
import com.aiwf.base.web.dto.StepResp;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClientException;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Service
public class JobService {
    private static final String JOB_DIRS_FAIL_ACTION = "JOB_DIRS_FAIL";
    private static final String FLOW_RUN_REQUEST_ACTION = "FLOW_RUN_REQUEST";
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
    private static final String LEGACY_FLOW_COMPATIBILITY_PATH = "run_flow_legacy";
    private static final String LEGACY_FLOW_REMOVE_BY = "2026-06-30";
    private static final String LEGACY_FLOW_KILL_CONDITION = "migrate Dify and all direct flow consumers to run-reference";
    private static final String LEGACY_FLOW_DISPATCH_SOURCE = "legacy_flow_dispatch";
    private static final String VERSION_REFERENCE_SOURCE = "version_reference";
    private static final String LEGACY_FLOW_REQUEST_KIND = "legacy_flow";
    private static final String REFERENCE_REQUEST_KIND = "reference";
    private static final String REFERENCE_ROUTE_STEP_ID = "run_reference";

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
            JobWorkspaceSupport.ensureJobDirs(jobsBusRoot, jobId);
        } catch (RuntimeException e) {
            recordJobDirFailure(jobId, defaultIfBlank(owner, "base"), e);
            throw ApiException.internalServerError(
                    "job_dirs_failed",
                    "failed to prepare job directories",
                    Map.of("job_id", jobId)
            );
        }
        return new JobCreateResp(
                jobId,
                owner,
                JobStatus.RUNNING.toDb(),
                Paths.get(JobWorkspaceSupport.jobsRoot(jobsBusRoot), jobId).toString(),
                null
        );
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

    public List<JobRunRecordResp> listRunHistory(int limit) {
        return jobs.listRecentJobs(limit).stream().map(this::toRunRecord).toList();
    }

    public JobRunRecordResp getRunRecord(String jobId) {
        return toRunRecord(requireJob(jobId));
    }

    public JobRunTimelineResp getRunTimeline(String jobId) {
        JobRunRecordResp record = toRunRecord(requireJob(jobId));
        @SuppressWarnings("unchecked")
        List<StepResp> steps = (List<StepResp>) record.result().getOrDefault("steps", List.of());
        List<JobTimelineItemResp> timeline = new ArrayList<>();
        for (StepResp step : steps) {
            timeline.add(new JobTimelineItemResp(
                    step.stepId(),
                    step.stepId(),
                    step.status(),
                    step.startedAt(),
                    step.endedAt(),
                    secondsBetween(step.startedAt(), step.endedAt()),
                    step.error()
            ));
        }
        timeline.sort((a, b) -> String.valueOf(a.startedAt()).compareTo(String.valueOf(b.startedAt())));
        return new JobRunTimelineResp(
                "lifecycle_run_timeline.v1",
                true,
                "base-java",
                "base-java.jobs",
                record.runId(),
                record.status(),
                timeline
        );
    }

    public JobFailureSummaryResp getFailureSummary(int limit) {
        List<JobRow> runs = jobs.listRecentJobs(limit);
        int failedRuns = 0;
        Map<String, Map<String, Object>> byNode = new LinkedHashMap<>();
        for (JobRow run : runs) {
            if (run.status() == JobStatus.FAILED) {
                failedRuns += 1;
            }
            for (StepResp step : jobs.listSteps(run.jobId()).stream().map(this::toStepResp).toList()) {
                if (!"FAILED".equalsIgnoreCase(step.status())) continue;
                Map<String, Object> current = byNode.computeIfAbsent(step.stepId(), key -> {
                    Map<String, Object> seed = new LinkedHashMap<>();
                    seed.put("failed", 0);
                    seed.put("samples", new ArrayList<String>());
                    return seed;
                });
                current.put("failed", ((Number) current.get("failed")).intValue() + 1);
                @SuppressWarnings("unchecked")
                List<String> samples = (List<String>) current.get("samples");
                if (samples.size() < 3 && step.error() != null && !step.error().isBlank()) {
                    samples.add(step.error().substring(0, Math.min(step.error().length(), 200)));
                }
            }
        }
        return new JobFailureSummaryResp(
                "lifecycle_failure_summary.v1",
                true,
                "base-java",
                "base-java.jobs",
                runs.size(),
                failedRuns,
                byNode
        );
    }

    public List<AuditEventResp> listAuditEvents(int limit, String action) {
        return jobs.listAuditEvents(limit, action).stream().map(this::toAuditEventResp).toList();
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
        Map<String, Object> normalizedParams = GlueRunParamsSupport.filterReservedKeys(params, RESERVED_GLUE_PARAM_KEYS);
        try {
            JobWorkspaceSupport.ensureJobDirs(jobsBusRoot, jobId);
        } catch (RuntimeException e) {
            recordJobDirFailure(jobId, effectiveActor, e);
            return GlueRunResult.failed(jobId, flow, "failed to prepare job directories");
        }

        try {
            GlueJobContext jobContext = JobWorkspaceSupport.buildJobContext(jobsBusRoot, jobId);
            jobs.audit(new AuditEvent(
                    jobId,
                    effectiveActor,
                    FLOW_RUN_REQUEST_ACTION,
                    flow,
                    JsonUtil.toJson(buildLegacyRunRequestDetail(flow, effectiveActor, effectiveRuleset, normalizedParams))
            ));
            return glue.runFlow(jobId, flow, new GlueRunFlowReq(
                    jobId,
                    flow,
                    effectiveActor,
                    effectiveRuleset,
                    newTraceId(),
                    jobContext,
                    normalizedParams
            ));
        } catch (RestClientException e) {
            jobs.audit(new AuditEvent(
                    jobId,
                    effectiveActor,
                    "FLOW_RUN_FAIL",
                    flow,
                    JsonUtil.toJson(buildRunFailureDetail(
                            LEGACY_FLOW_REQUEST_KIND,
                            flow,
                            Map.of(),
                            effectiveRuleset,
                            normalizedParams,
                            e.getMessage(),
                            Map.of(
                                    "compatibility_path", LEGACY_FLOW_COMPATIBILITY_PATH,
                                    "remove_by", LEGACY_FLOW_REMOVE_BY,
                                    "kill_condition", LEGACY_FLOW_KILL_CONDITION
                            )
                    ))
            ));
            jobStatus.onStepFail(jobId);
            return GlueRunResult.failed(jobId, flow, e.getMessage());
        }
    }

    public GlueRunResult runWorkflowReference(
            String jobId,
            String versionId,
            String actor,
            String rulesetVersion,
            Map<String, Object> params
    ) {
        String normalizedVersionId = defaultIfBlank(versionId, "");
        if (normalizedVersionId.isEmpty()) {
            throw ApiException.badRequest("workflow_reference_request_invalid", "version_id is required");
        }
        Map<String, Object> referenceParams = new LinkedHashMap<>();
        referenceParams.put("version_id", normalizedVersionId);
        referenceParams.put("published_version_id", normalizedVersionId);
        if (params != null && !params.isEmpty()) {
            referenceParams.putAll(params);
        }
        if (jobs.getJob(jobId) == null) {
            throw ApiException.notFound("job_not_found", "job not found", Map.of("job_id", jobId));
        }
        String effectiveActor = defaultIfBlank(actor, "glue");
        String effectiveRuleset = (rulesetVersion == null || rulesetVersion.isBlank()) ? "v1" : rulesetVersion;
        Map<String, Object> normalizedParams = GlueRunParamsSupport.filterReservedKeys(referenceParams, RESERVED_GLUE_PARAM_KEYS);
        try {
            JobWorkspaceSupport.ensureJobDirs(jobsBusRoot, jobId);
        } catch (RuntimeException e) {
            recordJobDirFailure(jobId, effectiveActor, e);
            return GlueRunResult.failed(jobId, "", "failed to prepare job directories");
        }
        try {
            GlueJobContext jobContext = JobWorkspaceSupport.buildJobContext(jobsBusRoot, jobId);
            jobs.audit(new AuditEvent(
                    jobId,
                    effectiveActor,
                    FLOW_RUN_REQUEST_ACTION,
                    REFERENCE_ROUTE_STEP_ID,
                    JsonUtil.toJson(buildReferenceRunRequestDetail(normalizedVersionId, effectiveActor, effectiveRuleset, normalizedParams))
            ));
            return glue.runReference(jobId, new GlueRunReferenceReq(
                    normalizedVersionId,
                    normalizedVersionId,
                    effectiveActor,
                    effectiveRuleset,
                    newTraceId(),
                    jobContext,
                    normalizedParams
            ));
        } catch (RestClientException e) {
            jobs.audit(new AuditEvent(
                    jobId,
                    effectiveActor,
                    "FLOW_RUN_FAIL",
                    REFERENCE_ROUTE_STEP_ID,
                    JsonUtil.toJson(buildRunFailureDetail(
                            REFERENCE_REQUEST_KIND,
                            "run-reference",
                            Map.of(
                                    "version_id", normalizedVersionId,
                                    "published_version_id", normalizedVersionId
                            ),
                            effectiveRuleset,
                            normalizedParams,
                            e.getMessage(),
                            Map.of("workflow_definition_source", VERSION_REFERENCE_SOURCE)
                    ))
            ));
            jobStatus.onStepFail(jobId);
            return GlueRunResult.failed(jobId, "", e.getMessage());
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

    private void recordJobDirFailure(String jobId, String actor, Exception error) {
        jobs.audit(new AuditEvent(jobId, actor, JOB_DIRS_FAIL_ACTION, null, defaultIfBlank(error.getMessage(), error.getClass().getSimpleName())));
        jobStatus.onStepFail(jobId);
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

    private AuditEventResp toAuditEventResp(AuditLogRow row) {
        return new AuditEventResp(
                "lifecycle_audit_event.v1",
                "base-java",
                "base-java.jobs",
                row.createdAt(),
                row.actor(),
                row.action(),
                row.jobId(),
                row.stepId(),
                JsonUtil.fromJson(row.detailJson())
        );
    }

    private JobRunRecordResp toRunRecord(JobRow row) {
        List<StepResp> steps = jobs.listSteps(row.jobId()).stream().map(this::toStepResp).toList();
        List<ArtifactResp> artifacts = jobs.listArtifacts(row.jobId()).stream().map(this::toArtifactResp).toList();
        Map<String, Object> requestMetadata = latestRunRequestMetadata(row.jobId());
        Map<String, Object> payload = normalizeRunRecordPayload(requestMetadata, steps);
        String runRequestKind = stringValue(requestMetadata.get("run_request_kind"), steps.isEmpty() ? LEGACY_FLOW_REQUEST_KIND : LEGACY_FLOW_REQUEST_KIND);
        String versionId = stringValue(requestMetadata.get("version_id"), "");
        String publishedVersionId = stringValue(requestMetadata.get("published_version_id"), "");
        String workflowDefinitionSource = stringValue(
                requestMetadata.get("workflow_definition_source"),
                REFERENCE_REQUEST_KIND.equals(runRequestKind) ? VERSION_REFERENCE_SOURCE : LEGACY_FLOW_DISPATCH_SOURCE
        );
        String workflowId = resolveWorkflowId(requestMetadata, steps);
        boolean ok = row.status() == JobStatus.DONE;
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("job_id", row.jobId());
        result.put("status", row.status().toDb());
        result.put("ok", ok);
        result.put("steps", steps);
        result.put("artifacts", artifacts);
        return new JobRunRecordResp(
                "lifecycle_run_record.v1",
                "base-java",
                "base-java.jobs",
                row.jobId(),
                row.createdAt(),
                runRequestKind,
                versionId,
                publishedVersionId,
                workflowDefinitionSource,
                workflowId,
                row.status().toDb(),
                ok,
                payload,
                Map.of(),
                result
        );
    }

    private double secondsBetween(Object startedAt, Object endedAt) {
        Instant start = toInstant(startedAt);
        Instant end = toInstant(endedAt);
        if (start == null || end == null) {
            return 0;
        }
        return Math.max(0, Duration.between(start, end).toMillis() / 1000.0);
    }

    private Instant toInstant(Object value) {
        if (value == null) return null;
        if (value instanceof Instant instant) return instant;
        if (value instanceof OffsetDateTime offsetDateTime) return offsetDateTime.toInstant();
        if (value instanceof LocalDateTime localDateTime) return localDateTime.toInstant(ZoneOffset.UTC);
        if (value instanceof java.sql.Timestamp timestamp) return timestamp.toInstant();
        if (value instanceof java.util.Date date) return date.toInstant();
        return null;
    }

    private static String defaultIfBlank(String value, String fallback) {
        if (value == null) {
            return fallback;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? fallback : trimmed;
    }

    private Map<String, Object> latestRunRequestMetadata(String jobId) {
        return jobs.listJobAuditEvents(jobId, 50).stream()
                .filter(item -> FLOW_RUN_REQUEST_ACTION.equalsIgnoreCase(String.valueOf(item.action())))
                .findFirst()
                .map(item -> JsonUtil.fromJsonObject(item.detailJson()))
                .orElse(Map.of());
    }

    private Map<String, Object> normalizeRunRecordPayload(Map<String, Object> requestMetadata, List<StepResp> steps) {
        String runRequestKind = stringValue(requestMetadata.get("run_request_kind"), "");
        if (REFERENCE_REQUEST_KIND.equals(runRequestKind)) {
            Map<String, Object> out = new LinkedHashMap<>();
            putIfPresent(out, "version_id", requestMetadata.get("version_id"));
            putIfPresent(out, "published_version_id", requestMetadata.get("published_version_id"));
            putIfPresent(out, "params", requestMetadata.get("params"));
            putIfPresent(out, "compatibility_path", requestMetadata.get("compatibility_path"));
            return out;
        }
        if (!requestMetadata.isEmpty()) {
            return new LinkedHashMap<>(requestMetadata);
        }
        return steps.isEmpty() ? Map.of() : JsonUtil.fromJsonObject(steps.get(0).paramsJson());
    }

    private String resolveWorkflowId(Map<String, Object> requestMetadata, List<StepResp> steps) {
        String workflowId = stringValue(requestMetadata.get("workflow_id"), "");
        if (!workflowId.isEmpty()) return workflowId;
        workflowId = stringValue(requestMetadata.get("published_version_id"), "");
        if (!workflowId.isEmpty()) return workflowId;
        workflowId = stringValue(requestMetadata.get("version_id"), "");
        if (!workflowId.isEmpty()) return workflowId;
        return steps.isEmpty() ? "" : defaultIfBlank(steps.get(0).stepId(), "");
    }

    private Map<String, Object> buildLegacyRunRequestDetail(
            String flow,
            String actor,
            String rulesetVersion,
            Map<String, Object> params
    ) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("run_request_kind", LEGACY_FLOW_REQUEST_KIND);
        detail.put("workflow_definition_source", LEGACY_FLOW_DISPATCH_SOURCE);
        detail.put("workflow_id", flow);
        detail.put("flow", flow);
        detail.put("actor", actor);
        detail.put("ruleset_version", rulesetVersion);
        detail.put("params", params);
        detail.put("compatibility_path", LEGACY_FLOW_COMPATIBILITY_PATH);
        detail.put("remove_by", LEGACY_FLOW_REMOVE_BY);
        detail.put("kill_condition", LEGACY_FLOW_KILL_CONDITION);
        return detail;
    }

    private Map<String, Object> buildReferenceRunRequestDetail(
            String versionId,
            String actor,
            String rulesetVersion,
            Map<String, Object> params
    ) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("run_request_kind", REFERENCE_REQUEST_KIND);
        detail.put("workflow_definition_source", VERSION_REFERENCE_SOURCE);
        detail.put("workflow_id", versionId);
        detail.put("version_id", versionId);
        detail.put("published_version_id", versionId);
        detail.put("actor", actor);
        detail.put("ruleset_version", rulesetVersion);
        detail.put("params", params);
        return detail;
    }

    private Map<String, Object> buildRunFailureDetail(
            String runRequestKind,
            String flow,
            Map<String, Object> referenceFields,
            String rulesetVersion,
            Map<String, Object> params,
            String error,
            Map<String, Object> extras
    ) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("run_request_kind", runRequestKind);
        detail.put("flow", flow);
        detail.put("ruleset_version", rulesetVersion);
        detail.put("params", params);
        detail.put("error", defaultIfBlank(error, "dispatch failed"));
        detail.putAll(referenceFields);
        detail.putAll(extras);
        return detail;
    }

    private void putIfPresent(Map<String, Object> target, String key, Object value) {
        if (value != null) {
            target.put(key, value);
        }
    }

    private String stringValue(Object value, String fallback) {
        if (value == null) return fallback;
        String normalized = String.valueOf(value).trim();
        return normalized.isEmpty() ? fallback : normalized;
    }
}
