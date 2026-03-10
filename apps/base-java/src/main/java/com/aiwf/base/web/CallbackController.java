package com.aiwf.base.web;

import com.aiwf.base.service.JobCallbackService;
import com.aiwf.base.service.JobService;
import com.aiwf.base.service.JsonUtil;
import com.aiwf.base.web.dto.ArtifactRegisterReq;
import com.aiwf.base.web.dto.ArtifactRegisterResp;
import com.aiwf.base.web.dto.OkResp;
import com.aiwf.base.web.dto.StepDoneCallbackReq;
import com.aiwf.base.web.dto.StepFailCallbackReq;
import com.aiwf.base.web.dto.StepFailResp;
import com.aiwf.base.web.dto.StepStartCallbackReq;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1")
public class CallbackController {

    private final JobCallbackService callbacks;
    private final JobService jobs;

    public CallbackController(JobCallbackService callbacks, JobService jobs) {
        this.callbacks = callbacks;
        this.jobs = jobs;
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/start", consumes = {"application/json", "*/*"})
    public OkResp stepStart(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepStartCallbackReq body
    ) {
        callbacks.stepStart(jobId, stepId, actor, body == null ? new StepStartCallbackReq() : body);
        return OkResp.success();
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/done", consumes = {"application/json", "*/*"})
    public OkResp stepDone(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepDoneCallbackReq body
    ) {
        callbacks.stepDone(jobId, stepId, actor, body == null ? new StepDoneCallbackReq() : body);
        return OkResp.success();
    }

    @PostMapping(value = "/jobs/{jobId}/steps/{stepId}/fail", consumes = {"application/json", "*/*"})
    public StepFailResp stepFail(
            @PathVariable @NotBlank String jobId,
            @PathVariable @NotBlank String stepId,
            @RequestParam(defaultValue = "glue") String actor,
            @RequestBody(required = false) StepFailCallbackReq body
    ) {
        StepFailCallbackReq req = body == null ? new StepFailCallbackReq() : body;
        String error = req.errorOrDefault("failed");
        return jobs.failStep(jobId, stepId, actor, error, JsonUtil.toJson(req.payload(error)));
    }

    @PostMapping(value = "/jobs/{jobId}/artifacts/register", consumes = {"application/json", "*/*"})
    public ArtifactRegisterResp registerArtifact(
            @PathVariable @NotBlank String jobId,
            @RequestParam(defaultValue = "glue") String actor,
            @Valid @RequestBody ArtifactRegisterReq body
    ) {
        return callbacks.registerArtifact(jobId, actor, body);
    }
}
