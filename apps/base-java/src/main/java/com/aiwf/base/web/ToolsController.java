package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import com.aiwf.base.web.dto.CreateJobPolicyReq;
import com.aiwf.base.web.dto.JobCreateResp;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/tools")
public class ToolsController {

    private final JobService jobs;

    public ToolsController(JobService jobs) {
        this.jobs = jobs;
    }

    @PostMapping("/create_job")
    public JobCreateResp createJob(
            @RequestParam(name = "owner", defaultValue = "local") @NotBlank String owner,
            @RequestBody(required = false) CreateJobPolicyReq body
    ) {
        return jobs.createJob(owner, body == null ? Map.of() : body.policy());
    }
}
