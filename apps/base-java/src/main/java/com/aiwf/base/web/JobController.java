package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.dto.AuditEventResp;
import com.aiwf.base.web.dto.ArtifactResp;
import com.aiwf.base.web.dto.CreateJobPolicyReq;
import com.aiwf.base.web.dto.GlueHealthResp;
import com.aiwf.base.web.dto.JobFailureSummaryResp;
import com.aiwf.base.web.dto.JobCreateResp;
import com.aiwf.base.web.dto.JobDetailsResp;
import com.aiwf.base.web.dto.JobRunRecordResp;
import com.aiwf.base.web.dto.JobRunTimelineResp;
import com.aiwf.base.web.dto.RunFlowReq;
import com.aiwf.base.web.dto.StepResp;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    @GetMapping("/glue/health")
    public GlueHealthResp glueHealth() {
        var glue = jobs.glueHealth();
        return new GlueHealthResp(glue.isOk(), glue);
    }

    @PostMapping("/create")
    public JobCreateResp createJob(
            @RequestParam(name = "owner", defaultValue = "local") @NotBlank String owner,
            @RequestBody(required = false) CreateJobPolicyReq body
    ) {
        return jobs.createJob(owner, body == null ? Map.of() : body.policy());
    }

    @GetMapping("/{jobId}")
    public JobDetailsResp getJob(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.getJob(jobId);
    }

    @GetMapping("/history")
    public java.util.List<JobRunRecordResp> listRunHistory(
            @RequestParam(name = "limit", defaultValue = "200") int limit
    ) {
        return jobs.listRunHistory(limit);
    }

    @GetMapping("/{jobId}/record")
    public JobRunRecordResp getRunRecord(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.getRunRecord(jobId);
    }

    @GetMapping("/{jobId}/steps")
    public java.util.List<StepResp> listSteps(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listSteps(jobId);
    }

    @GetMapping("/{jobId}/artifacts")
    public java.util.List<ArtifactResp> listArtifacts(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listArtifacts(jobId);
    }

    @GetMapping("/{jobId}/timeline")
    public JobRunTimelineResp getRunTimeline(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.getRunTimeline(jobId);
    }

    @GetMapping("/failure-summary")
    public JobFailureSummaryResp getFailureSummary(
            @RequestParam(name = "limit", defaultValue = "400") int limit
    ) {
        return jobs.getFailureSummary(limit);
    }

    @GetMapping("/audit-events")
    public java.util.List<AuditEventResp> listAuditEvents(
            @RequestParam(name = "limit", defaultValue = "200") int limit,
            @RequestParam(name = "action", defaultValue = "") String action
    ) {
        return jobs.listAuditEvents(limit, action);
    }

    @PostMapping("/{jobId}/run/{flow}")
    public GlueRunResult runFlow(
            @PathVariable("jobId") @NotBlank String jobId,
            @PathVariable("flow") @NotBlank String flow,
            @RequestBody RunFlowReq body
    ) {
        return jobs.runFlow(
                jobId,
                flow,
                body.actorOrDefault("local"),
                body.rulesetVersionOrDefault("v1"),
                body.resolvedParams()
        );
    }
}
