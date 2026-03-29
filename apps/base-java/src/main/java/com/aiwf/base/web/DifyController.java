package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.DifyRunCleaningResp;
import com.aiwf.base.web.dto.DifyRunCleaningReq;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/integrations/dify")
public class DifyController {
    private static final String CLEANING_COMPATIBILITY_VERSION_ID = "ver_cleaning_compat_001";

    private final JobService jobs;

    public DifyController(JobService jobs) {
        this.jobs = jobs;
    }

    /**
     * Dify-friendly single-call endpoint:
     * create job -> run cleaning -> return run/steps/artifacts snapshot.
     */
    @PostMapping("/run_cleaning")
    public DifyRunCleaningResp runCleaning(
            @RequestBody(required = false) DifyRunCleaningReq body
    ) {
        DifyRunCleaningReq req = body == null ? new DifyRunCleaningReq(null, null, null, null, null) : body;

        var job = jobs.createJob(req.ownerOrDefault("dify"), req.policyOrEmpty());
        String jobId = job.jobId();
        if (jobId == null || jobId.isBlank()) {
            throw ApiException.internalServerError(
                    "job_create_failed",
                    "create_job returned empty job_id",
                    Map.of("owner", req.ownerOrDefault("dify"))
            );
        }

        var run = jobs.runWorkflowReference(
                jobId,
                CLEANING_COMPATIBILITY_VERSION_ID,
                req.actorOrDefault("dify"),
                req.rulesetVersionOrDefault("v1"),
                req.paramsOrEmpty()
        );
        return new DifyRunCleaningResp(run.isOk(), jobId, job, run, jobs.listSteps(jobId), jobs.listArtifacts(jobId));
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("ok", true, "service", "base-java", "integration", "dify");
    }
}
