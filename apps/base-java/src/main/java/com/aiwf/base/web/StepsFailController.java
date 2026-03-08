package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.StepFailReq;
import com.aiwf.base.web.dto.StepFailResp;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/steps")
public class StepsFailController {

    private final JobService jobService;

    public StepsFailController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping("/{jobId}/{stepId}/fail")
    public StepFailResp stepFail(
            @PathVariable("jobId") @NotBlank String jobId,
            @PathVariable("stepId") @NotBlank String stepId,
            @RequestParam(name = "actor", defaultValue = "local") String actor,
            @RequestBody StepFailReq req
    ) {
        return jobService.stepFail(jobId, stepId, actor, req);
    }
}
