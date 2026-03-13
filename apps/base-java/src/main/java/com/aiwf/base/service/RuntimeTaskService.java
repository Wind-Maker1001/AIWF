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
import java.util.Objects;

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
        String idempotencyKey = trimToNull(body.idempotencyKey());
        long createdAt = body.createdAt() == null ? 0L : body.createdAt();
        long updatedAt = body.updatedAt() == null ? 0L : body.updatedAt();
        int attempts = body.attempts() == null ? 0 : Math.max(0, body.attempts());
        if (createdAt <= 0) {
            createdAt = updatedAt > 0 ? updatedAt : (System.currentTimeMillis() / 1000L);
        }
        if (updatedAt <= 0) {
            updatedAt = createdAt;
        }

        try {
            RuntimeTaskRow existing = tasks.getTask(taskId);
            if (existing == null && idempotencyKey != null) {
                existing = tasks.getTaskByTenantOperatorAndIdempotencyKey(tenantId, operator, idempotencyKey);
                if (existing != null) {
                    return new RuntimeTaskUpsertResp(true, existing.taskId(), existing.tenantId(), existing.status().toDb());
                }
            }
            if (existing != null) {
                if (!Objects.equals(existing.tenantId(), tenantId)) {
                    throw ApiException.conflict(
                            "runtime_task_tenant_conflict",
                            "runtime task tenant cannot change",
                            Map.of("task_id", taskId, "tenant_id", existing.tenantId())
                    );
                }
                if (!Objects.equals(existing.operator(), operator)) {
                    throw ApiException.conflict(
                            "runtime_task_operator_conflict",
                            "runtime task operator cannot change",
                            Map.of("task_id", taskId, "operator", existing.operator())
                    );
                }
                if (existing.idempotencyKey() != null && idempotencyKey != null && !Objects.equals(existing.idempotencyKey(), idempotencyKey)) {
                    throw ApiException.conflict(
                            "runtime_task_idempotency_conflict",
                            "runtime task idempotency key cannot change",
                            Map.of("task_id", taskId, "idempotency_key", existing.idempotencyKey())
                    );
                }
                if (updatedAt < existing.updatedAtEpoch()) {
                    return new RuntimeTaskUpsertResp(true, existing.taskId(), existing.tenantId(), existing.status().toDb());
                }
                if (!existing.status().canTransitionTo(status)) {
                    return new RuntimeTaskUpsertResp(true, existing.taskId(), existing.tenantId(), existing.status().toDb());
                }
                createdAt = existing.createdAtEpoch();
                if (idempotencyKey == null) {
                    idempotencyKey = existing.idempotencyKey();
                }
                attempts = Math.max(existing.attempts(), attempts);
            }
            tasks.upsertTask(
                    taskId,
                    tenantId,
                    operator,
                    status,
                    createdAt,
                    updatedAt,
                    JsonUtil.toJsonOrNull(body.result()),
                    trimToNull(body.error()),
                    defaultIfBlank(body.source(), "accel-rust"),
                    idempotencyKey,
                    attempts
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
                row.source(),
                row.idempotencyKey(),
                row.attempts()
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
