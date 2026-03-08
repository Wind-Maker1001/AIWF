package com.aiwf.base.web;

import com.aiwf.base.db.RuntimeTaskRepository;
import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.service.JsonUtil;
import com.aiwf.base.web.dto.RuntimeTaskCancelResp;
import com.aiwf.base.web.dto.RuntimeTaskGetResp;
import com.aiwf.base.web.dto.RuntimeTaskListResp;
import com.aiwf.base.web.dto.RuntimeTaskResp;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;
import jakarta.validation.constraints.NotBlank;
import org.springframework.dao.DataAccessException;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/runtime")
public class RuntimeTaskController {
    private final RuntimeTaskRepository tasks;

    public RuntimeTaskController(RuntimeTaskRepository tasks) {
        this.tasks = tasks;
    }

    @PostMapping(value = "/tasks/upsert", consumes = {"application/json", "*/*"})
    public RuntimeTaskUpsertResp upsertTask(@RequestBody RuntimeTaskUpsertReq body) {
        String taskId = trimToNull(body.taskId());
        if (taskId == null) {
            throw ApiException.badRequest("task_id_required", "task_id required");
        }

        String operator = defaultIfBlank(body.operator(), "transform_rows_v2");
        String status = defaultIfBlank(body.status(), "queued");
        long createdAt = body.createdAt() == null ? 0L : body.createdAt();
        long updatedAt = body.updatedAt() == null ? 0L : body.updatedAt();
        if (createdAt <= 0) {
            createdAt = updatedAt > 0 ? updatedAt : (System.currentTimeMillis() / 1000L);
        }
        if (updatedAt <= 0) {
            updatedAt = createdAt;
        }

        try {
            tasks.upsertTask(
                    taskId,
                    defaultIfBlank(body.tenantId(), "default"),
                    operator,
                    status,
                    createdAt,
                    updatedAt,
                    JsonUtil.toJsonOrNull(body.result()),
                    trimToNull(body.error()),
                    defaultIfBlank(body.source(), "accel-rust")
            );
            return new RuntimeTaskUpsertResp(true, taskId, defaultIfBlank(body.tenantId(), "default"), status);
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", String.valueOf(e.getMostSpecificCause()));
        }
    }

    @GetMapping("/tasks/{taskId}")
    public RuntimeTaskGetResp getTask(@PathVariable("taskId") @NotBlank String taskId) {
        RuntimeTaskRow row;
        try {
            row = tasks.getTask(taskId);
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", String.valueOf(e.getMostSpecificCause()));
        }
        if (row == null) {
            throw ApiException.notFound("task_not_found", "task not found", Map.of("task_id", taskId));
        }
        return new RuntimeTaskGetResp(true, toRuntimeTaskResp(row));
    }

    @PostMapping("/tasks/{taskId}/cancel")
    public RuntimeTaskCancelResp cancelTask(@PathVariable("taskId") @NotBlank String taskId) {
        long now = System.currentTimeMillis() / 1000L;
        RuntimeTaskCancelResult out;
        try {
            out = tasks.cancelTask(taskId, now);
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", String.valueOf(e.getMostSpecificCause()));
        }
        RuntimeTaskRow task = out.task();
        if (task == null) {
            throw ApiException.notFound("task_not_found", "task not found", Map.of("task_id", taskId));
        }
        return new RuntimeTaskCancelResp(true, taskId, out.cancelled(), task.status());
    }

    @GetMapping("/tasks")
    public RuntimeTaskListResp listTasksByTenant(
            @RequestParam(name = "tenant_id", defaultValue = "default") String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        try {
            return new RuntimeTaskListResp(
                    true,
                    tenantId,
                    tasks.listTasksByTenant(tenantId, limit).stream().map(this::toRuntimeTaskResp).toList()
            );
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", String.valueOf(e.getMostSpecificCause()));
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String defaultIfBlank(String value, String fallback) {
        String trimmed = trimToNull(value);
        return trimmed == null ? fallback : trimmed;
    }

    private RuntimeTaskResp toRuntimeTaskResp(RuntimeTaskRow row) {
        return new RuntimeTaskResp(
                row.taskId(),
                row.tenantId(),
                row.operator(),
                row.status(),
                row.createdAtEpoch(),
                row.updatedAtEpoch(),
                row.resultJson(),
                row.error(),
                row.source()
        );
    }
}
