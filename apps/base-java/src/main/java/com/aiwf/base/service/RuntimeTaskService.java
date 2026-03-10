package com.aiwf.base.service;

import com.aiwf.base.db.RuntimeTaskRepository;
import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.RuntimeTaskCancelResp;
import com.aiwf.base.web.dto.RuntimeTaskGetResp;
import com.aiwf.base.web.dto.RuntimeTaskListResp;
import com.aiwf.base.web.dto.RuntimeTaskResp;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class RuntimeTaskService {

    private final RuntimeTaskRepository tasks;

    public RuntimeTaskService(RuntimeTaskRepository tasks) {
        this.tasks = tasks;
    }

    public RuntimeTaskUpsertResp upsertTask(RuntimeTaskUpsertReq body) {
        String taskId = trimToNull(body.taskId());
        if (taskId == null) {
            throw ApiException.badRequest("task_id_required", "task_id required");
        }

        String tenantId = defaultIfBlank(body.tenantId(), "default");
        String operator = defaultIfBlank(body.operator(), "transform_rows_v2");
        RuntimeTaskStatus status = parseStatus(body.status(), RuntimeTaskStatus.QUEUED);
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
                    tenantId,
                    operator,
                    status,
                    createdAt,
                    updatedAt,
                    JsonUtil.toJsonOrNull(body.result()),
                    trimToNull(body.error()),
                    defaultIfBlank(body.source(), "accel-rust")
            );
            return new RuntimeTaskUpsertResp(true, taskId, tenantId, status.toDb());
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", String.valueOf(e.getMostSpecificCause()));
        }
    }

    public RuntimeTaskGetResp getTask(String taskId) {
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

    public RuntimeTaskCancelResp cancelTask(String taskId) {
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
        return new RuntimeTaskCancelResp(true, taskId, out.cancelled(), task.status().toDb());
    }

    public RuntimeTaskListResp listTasksByTenant(String tenantId, int limit) {
        String effectiveTenantId = defaultIfBlank(tenantId, "default");
        try {
            return new RuntimeTaskListResp(
                    true,
                    effectiveTenantId,
                    tasks.listTasksByTenant(effectiveTenantId, limit).stream().map(this::toRuntimeTaskResp).toList()
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
                row.status().toDb(),
                row.createdAtEpoch(),
                row.updatedAtEpoch(),
                row.resultJson(),
                row.error(),
                row.source()
        );
    }

    private RuntimeTaskStatus parseStatus(String value, RuntimeTaskStatus fallback) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            return fallback;
        }
        try {
            return RuntimeTaskStatus.fromDb(trimmed);
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest(
                    "runtime_task_status_invalid",
                    "invalid runtime task status",
                    Map.of("status", trimmed)
            );
        }
    }
}
