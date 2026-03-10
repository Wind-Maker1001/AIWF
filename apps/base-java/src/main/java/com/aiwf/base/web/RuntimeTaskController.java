package com.aiwf.base.web;

import com.aiwf.base.service.RuntimeTaskService;
import com.aiwf.base.web.dto.RuntimeTaskCancelResp;
import com.aiwf.base.web.dto.RuntimeTaskGetResp;
import com.aiwf.base.web.dto.RuntimeTaskListResp;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/runtime")
public class RuntimeTaskController {
    private final RuntimeTaskService tasks;

    public RuntimeTaskController(RuntimeTaskService tasks) {
        this.tasks = tasks;
    }

    @PostMapping(value = "/tasks/upsert", consumes = {"application/json", "*/*"})
    public RuntimeTaskUpsertResp upsertTask(@RequestBody RuntimeTaskUpsertReq body) {
        return tasks.upsertTask(body);
    }

    @GetMapping("/tasks/{taskId}")
    public RuntimeTaskGetResp getTask(@PathVariable("taskId") @NotBlank String taskId) {
        return tasks.getTask(taskId);
    }

    @PostMapping("/tasks/{taskId}/cancel")
    public RuntimeTaskCancelResp cancelTask(@PathVariable("taskId") @NotBlank String taskId) {
        return tasks.cancelTask(taskId);
    }

    @GetMapping("/tasks")
    public RuntimeTaskListResp listTasksByTenant(
            @RequestParam(name = "tenant_id", defaultValue = "default") String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        return tasks.listTasksByTenant(tenantId, limit);
    }
}
