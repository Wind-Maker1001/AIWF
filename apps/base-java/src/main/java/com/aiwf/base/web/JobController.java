package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    @GetMapping("/glue/health")
    public Map<String, Object> glueHealth() {
        return Map.of("ok", true, "glue", jobs.glueHealth());
    }

    @PostMapping("/create")
    public Map<String, Object> createJob(
            @RequestParam(name = "owner", defaultValue = "local") @NotBlank String owner,
            @RequestBody(required = false) Map<String, Object> policy
    ) {
        if (policy == null) {
            policy = Map.of();
        }
        return jobs.createJob(owner, policy);
    }

    @GetMapping("/{jobId}")
    public Map<String, Object> getJob(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.getJob(jobId);
    }

    @GetMapping("/{jobId}/steps")
    public Object listSteps(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listSteps(jobId);
    }

    @GetMapping("/{jobId}/artifacts")
    public Object listArtifacts(@PathVariable("jobId") @NotBlank String jobId) {
        return jobs.listArtifacts(jobId);
    }

    @PostMapping("/{jobId}/run/{flow}")
    public Map<String, Object> runFlow(
            @PathVariable("jobId") @NotBlank String jobId,
            @PathVariable("flow") @NotBlank String flow,
            @RequestBody Map<String, Object> body
    ) {
        String actor = String.valueOf(body.getOrDefault("actor", "local"));
        String rulesetVersion = String.valueOf(body.getOrDefault("ruleset_version", "v1"));

        Object paramsObj = body.get("params");
        Map<String, Object> params = new HashMap<>();
        if (paramsObj instanceof Map<?, ?> p) {
            for (Map.Entry<?, ?> e : p.entrySet()) {
                if (e.getKey() != null) {
                    params.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
        } else {
            for (Map.Entry<String, Object> e : body.entrySet()) {
                String k = e.getKey();
                if (!"actor".equals(k) && !"ruleset_version".equals(k)) {
                    params.put(k, e.getValue());
                }
            }
        }

        return jobs.runFlow(jobId, flow, actor, rulesetVersion, params);
    }
}