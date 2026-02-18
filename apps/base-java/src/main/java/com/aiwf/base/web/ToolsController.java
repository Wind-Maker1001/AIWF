package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
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

    /**
     * 兼容你的调用：
     * POST /api/v1/tools/create_job?owner=local
     * body: {}
     */
    @PostMapping("/create_job")
    public Map<String, Object> createJob(
            @RequestParam(name = "owner", defaultValue = "local") @NotBlank String owner,
            @RequestBody(required = false) Map<String, Object> policy
    ) {
        if (policy == null) policy = Map.of();
        return jobs.createJob(owner, policy);
    }
}
