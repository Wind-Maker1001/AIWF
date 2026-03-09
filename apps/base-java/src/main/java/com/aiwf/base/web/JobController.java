package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.glue.GlueRunResult;
import com.aiwf.base.web.dto.CreateJobPolicyReq;
import com.aiwf.base.web.dto.GlueHealthResp;
import com.aiwf.base.web.dto.JobCreateResp;
import com.aiwf.base.web.dto.JobDetailsResp;
import com.aiwf.base.web.dto.StepResp;
import com.aiwf.base.web.dto.ArtifactResp;
import com.aiwf.base.web.dto.RunFlowReq;
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

    @GetMapping("/{jobId}/steps")
    public java.util.List<StepResp> listSteps(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listSteps(jobId);
    }

    @GetMapping("/{jobId}/artifacts")
    public java.util.List<ArtifactResp> listArtifacts(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listArtifacts(jobId);
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
