package com.aiwf.base.service;

import com.aiwf.base.db.AiWfDao;
import com.aiwf.base.web.dto.StepFailReq;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class JobService {

    private final AiWfDao dao;
    private final ObjectMapper om;
    private final RestTemplate http;

    @Value("${aiwf.bus:R:\\aiwf}")
    private String jobsBusRoot;

    @Value("${aiwf.glueUrl:http://127.0.0.1:18081}")
    private String glueBaseUrl;

    public JobService(AiWfDao dao, ObjectMapper om, RestTemplateBuilder rtb) {
        this.dao = dao;
        this.om = om;
        this.http = rtb
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(120))
                .build();
    }

    public Map<String, Object> glueHealth() {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = http.getForObject(glueBaseUrl + "/health", Map.class);
            return resp == null ? Map.of("ok", false) : resp;
        } catch (RestClientException e) {
            return Map.of("ok", false, "error", e.getMessage());
        }
    }

    public Map<String, Object> createJob(String owner) {
        String jobId = dao.createJob(owner);
        ensureJobDirs(jobId);

        Map<String, Object> out = new HashMap<>();
        out.put("job_id", jobId);
        out.put("owner", owner);
        out.put("status", "RUNNING");
        out.put("job_root", Paths.get(jobsRoot(), jobId).toString());
        return out;
    }

    public Map<String, Object> createJob(String owner, Map<String, Object> policy) {
        Map<String, Object> out = createJob(owner);
        if (policy != null) out.put("policy", policy);
        return out;
    }

    public Map<String, Object> getJob(String jobId) {
        Map<String, Object> job = dao.getJob(jobId);
        return job == null ? Map.of("ok", false, "error", "job not found") : job;
    }

    public List<Map<String, Object>> listSteps(String jobId) {
        return dao.listSteps(jobId);
    }

    public List<Map<String, Object>> listArtifacts(String jobId) {
        return dao.listArtifacts(jobId);
    }

    public Map<String, Object> runFlow(
            String jobId,
            String flow,
            String actor,
            String rulesetVersion,
            Map<String, Object> params
    ) {
        ensureJobDirs(jobId);

        String url = glueBaseUrl + "/jobs/" + jobId + "/run/" + flow;

        Map<String, Object> body = new HashMap<>();
        body.put("actor", actor == null ? "glue" : actor);
        body.put("ruleset_version", (rulesetVersion == null || rulesetVersion.isBlank()) ? "v1" : rulesetVersion);
        if (params != null) body.put("params", params);

        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> resp = http.postForObject(url, body, Map.class);
            return resp == null ? Map.of("ok", false, "error", "empty response from glue") : resp;
        } catch (RestClientException e) {
            dao.audit(jobId, actor == null ? "base" : actor, "FLOW_RUN_FAIL", flow, e.getMessage());
            return Map.of("ok", false, "job_id", jobId, "flow", flow, "error", e.getMessage());
        }
    }

    public Map<String, Object> runFlow(
            String jobId,
            String flow,
            String actor,
            Map<String, Object> params
    ) {
        return runFlow(jobId, flow, actor, "v1", params);
    }

    public Map<String, Object> stepFail(String jobId, String stepId, String actor, StepFailReq req) {
        String msg = (req == null) ? "manual stepFail" : (req.error() == null ? "manual stepFail" : req.error());
        dao.markStepFailed(jobId, stepId, msg);
        dao.audit(jobId, actor == null ? "manual" : actor, "STEP_FAIL", stepId, msg);
        return Map.of("ok", true, "job_id", jobId, "step_id", stepId);
    }

    private void ensureJobDirs(String jobId) {
        try {
            Path jobRoot = Paths.get(jobsRoot(), jobId);
            Files.createDirectories(jobRoot);
            Files.createDirectories(jobRoot.resolve("stage"));
            Files.createDirectories(jobRoot.resolve("artifacts"));
            Files.createDirectories(jobRoot.resolve("evidence"));
        } catch (Exception e) {
            dao.audit(jobId, "base", "JOB_DIRS_WARN", null, e.getMessage());
        }
    }

    private String jobsRoot() {
        return Paths.get(jobsBusRoot, "jobs").toString();
    }
}