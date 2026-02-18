package com.aiwf.base.web;

import com.aiwf.base.db.AiWfDao;
import com.aiwf.base.service.JobStatusService;
import com.aiwf.base.service.JsonUtil;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class CallbackController {

    private final AiWfDao dao;
    private final JobStatusService jobStatus;

    public CallbackController(AiWfDao dao, JobStatusService jobStatus) {
        this.dao = dao;
        this.jobStatus = jobStatus;
    }

    // ---- Steps ----

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/start", consumes = {"application/json", "*/*"})
    public Map<String, Object> stepStart(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) Map<String, Object> body
    ) {
        jobStatus.onStepStart(jobId);

        String inputUri = body == null ? "" : String.valueOf(body.getOrDefault("input_uri", ""));
        String outputUri = body == null ? "" : String.valueOf(body.getOrDefault("output_uri", ""));
        String rulesetVersion = body == null ? null : (body.get("ruleset_version") == null ? null : String.valueOf(body.get("ruleset_version")));
        String paramsJson = JsonUtil.toJson(body == null ? Map.of() : body);

        dao.upsertStepRunning(jobId, stepId, inputUri, outputUri, rulesetVersion, paramsJson);
        dao.audit(actor, "STEP_START", jobId, stepId, paramsJson);
        return Map.of("ok", true);
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/done", consumes = {"application/json", "*/*"})
    public Map<String, Object> stepDone(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) Map<String, Object> body
    ) {
        String outputHash = body == null ? null : (body.get("output_hash") == null ? null : String.valueOf(body.get("output_hash")));
        String detailJson = JsonUtil.toJson(body == null ? Map.of() : body);

        dao.markStepDone(jobId, stepId, outputHash);
        dao.audit(actor, "STEP_DONE", jobId, stepId, detailJson);

        jobStatus.onStepDone(jobId);
        return Map.of("ok", true);
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/fail", consumes = {"application/json", "*/*"})
    public Map<String, Object> stepFail(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) Map<String, Object> body
    ) {
        String error = body == null ? "failed" : String.valueOf(body.getOrDefault("error", "failed"));
        String detailJson = JsonUtil.toJson(body == null ? Map.of("error", error) : body);

        dao.markStepFailed(jobId, stepId, error);
        dao.audit(actor, "STEP_FAIL", jobId, stepId, detailJson);

        jobStatus.onStepFail(jobId);
        return Map.of("ok", true);
    }

    // ---- Artifacts ----

    @PostMapping(value = "/jobs/{jobId}/artifacts/register", consumes = {"application/json", "*/*"})
    public Map<String, Object> registerArtifact(
            @PathVariable @NotBlank String jobId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody Map<String, Object> body
    ) {
        String artifactId = String.valueOf(body.get("artifact_id"));
        String kind = String.valueOf(body.get("kind"));   // csv/xlsx/docx/pptx/powerbi/...
        String path = String.valueOf(body.get("path"));   // Recommended: <AIWF_BUS>\\jobs\\<id>\\artifacts\\...
        String sha256 = body.get("sha256") == null ? null : String.valueOf(body.get("sha256"));
        String bindingJson = body.get("binding_json") == null ? null : JsonUtil.toJson(body.get("binding_json"));

        dao.upsertArtifact(jobId, artifactId, kind, path, sha256, bindingJson);
        dao.audit(actor, "ARTIFACT_REGISTER", jobId, null, JsonUtil.toJson(body));
        return Map.of("ok", true, "artifact_id", artifactId);
    }
}
