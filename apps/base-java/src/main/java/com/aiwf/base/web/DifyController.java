package com.aiwf.base.web;

import com.aiwf.base.service.JobService;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/integrations/dify")
public class DifyController {

    private final JobService jobs;

    public DifyController(JobService jobs) {
        this.jobs = jobs;
    }

    /**
     * Dify-friendly single-call endpoint:
     * create job -> run cleaning -> return run/steps/artifacts snapshot.
     */
    @PostMapping("/run_cleaning")
    public Map<String, Object> runCleaning(
            @RequestBody(required = false) Map<String, Object> body
    ) {
        Map<String, Object> req = body == null ? Map.of() : body;

        String owner = readString(req, "owner", "dify");
        String actor = readString(req, "actor", "dify");
        String rulesetVersion = readString(req, "ruleset_version", "v1");

        Map<String, Object> policy = new HashMap<>();
        Object policyObj = req.get("policy");
        if (policyObj instanceof Map<?, ?> p) {
            for (Map.Entry<?, ?> e : p.entrySet()) {
                if (e.getKey() != null) {
                    policy.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
        }

        Map<String, Object> params = new HashMap<>();
        Object paramsObj = req.get("params");
        if (paramsObj instanceof Map<?, ?> p) {
            for (Map.Entry<?, ?> e : p.entrySet()) {
                if (e.getKey() != null) {
                    params.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
        }

        Map<String, Object> job = jobs.createJob(owner, policy);
        String jobId = String.valueOf(job.get("job_id"));
        if (jobId == null || jobId.isBlank()) {
            return Map.of("ok", false, "error", "create_job failed: empty job_id");
        }

        Map<String, Object> run = jobs.runFlow(jobId, "cleaning", actor, rulesetVersion, params);
        boolean ok = Boolean.TRUE.equals(run.get("ok"));

        Map<String, Object> out = new HashMap<>();
        out.put("ok", ok);
        out.put("job_id", jobId);
        out.put("job", job);
        out.put("run", run);
        out.put("steps", jobs.listSteps(jobId));
        out.put("artifacts", jobs.listArtifacts(jobId));
        return out;
    }

    private static String readString(Map<String, Object> req, String key, String defaultValue) {
        Object raw = req.get(key);
        if (raw == null) return defaultValue;
        String s = String.valueOf(raw).trim();
        if (s.isEmpty() || "null".equalsIgnoreCase(s)) return defaultValue;
        return s;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("ok", true, "service", "base-java", "integration", "dify");
    }
}
