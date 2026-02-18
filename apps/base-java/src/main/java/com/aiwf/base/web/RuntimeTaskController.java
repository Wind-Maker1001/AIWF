package com.aiwf.base.web;

import com.aiwf.base.db.AiWfDao;
import jakarta.validation.constraints.NotBlank;
import org.springframework.dao.DataAccessException;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/runtime")
public class RuntimeTaskController {
    private final AiWfDao dao;

    public RuntimeTaskController(AiWfDao dao) {
        this.dao = dao;
    }

    @PostMapping(value = "/tasks/upsert", consumes = {"application/json", "*/*"})
    public Map<String, Object> upsertTask(@RequestBody Map<String, Object> body) {
        String taskId = asString(body.get("task_id"));
        if (taskId == null || taskId.isBlank()) {
            return Map.of("ok", false, "error", "task_id required");
        }
        String operator = asString(body.get("operator"));
        if (operator == null || operator.isBlank()) operator = "transform_rows_v2";
        String status = asString(body.get("status"));
        if (status == null || status.isBlank()) status = "queued";
        long createdAt = asLong(body.get("created_at"));
        long updatedAt = asLong(body.get("updated_at"));
        if (createdAt <= 0) createdAt = updatedAt > 0 ? updatedAt : (System.currentTimeMillis() / 1000L);
        if (updatedAt <= 0) updatedAt = createdAt;
        String resultJson = body.get("result") == null ? null : dao.toJsonOrNull(body.get("result"));
        String error = asString(body.get("error"));
        String source = asString(body.get("source"));
        if (source == null || source.isBlank()) source = "accel-rust";
        String tenantId = asString(body.get("tenant_id"));
        if (tenantId == null || tenantId.isBlank()) tenantId = "default";
        try {
            dao.upsertWorkflowTask(taskId, tenantId, operator, status, createdAt, updatedAt, resultJson, error, source);
            return Map.of("ok", true, "task_id", taskId, "tenant_id", tenantId, "status", status);
        } catch (DataAccessException e) {
            return Map.of("ok", false, "error", "runtime_task_store_unavailable", "message", String.valueOf(e.getMostSpecificCause()));
        }
    }

    @GetMapping("/tasks/{taskId}")
    public Map<String, Object> getTask(@PathVariable("taskId") @NotBlank String taskId) {
        Map<String, Object> row;
        try {
            row = dao.getWorkflowTask(taskId);
        } catch (DataAccessException e) {
            return Map.of("ok", false, "error", "runtime_task_store_unavailable", "message", String.valueOf(e.getMostSpecificCause()));
        }
        if (row == null || row.isEmpty()) {
            return Map.of("ok", false, "error", "task_not_found", "task_id", taskId);
        }
        return Map.of("ok", true, "task", row);
    }

    @PostMapping("/tasks/{taskId}/cancel")
    public Map<String, Object> cancelTask(@PathVariable("taskId") @NotBlank String taskId) {
        long now = System.currentTimeMillis() / 1000L;
        Map<String, Object> out;
        try {
            out = dao.cancelWorkflowTask(taskId, now);
        } catch (DataAccessException e) {
            return Map.of("ok", false, "error", "runtime_task_store_unavailable", "message", String.valueOf(e.getMostSpecificCause()));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> task = (Map<String, Object>) out.get("task");
        if (task == null || task.isEmpty()) {
            return Map.of("ok", false, "error", "task_not_found", "task_id", taskId);
        }
        return Map.of(
                "ok", true,
                "task_id", taskId,
                "cancelled", Boolean.TRUE.equals(out.get("cancelled")),
                "status", String.valueOf(task.get("status"))
        );
    }

    @GetMapping("/tasks")
    public Map<String, Object> listTasksByTenant(
            @RequestParam(name = "tenant_id", defaultValue = "default") String tenantId,
            @RequestParam(name = "limit", defaultValue = "100") int limit
    ) {
        try {
            return Map.of("ok", true, "tenant_id", tenantId, "tasks", dao.listWorkflowTasksByTenant(tenantId, limit));
        } catch (DataAccessException e) {
            return Map.of("ok", false, "error", "runtime_task_store_unavailable", "message", String.valueOf(e.getMostSpecificCause()));
        }
    }

    private static String asString(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    private static long asLong(Object v) {
        if (v == null) return 0L;
        if (v instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(String.valueOf(v));
        } catch (Exception e) {
            return 0L;
        }
    }
}
