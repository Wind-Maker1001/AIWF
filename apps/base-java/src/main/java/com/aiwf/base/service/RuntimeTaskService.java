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
        long nowEpoch = System.currentTimeMillis() / 1000L;
        RuntimeTaskServiceSupport.PreparedRuntimeTaskUpsert prepared = RuntimeTaskServiceSupport.prepareUpsert(
                body,
                JsonUtil.toJsonOrNull(body.result()),
                nowEpoch
        );

        try {
            RuntimeTaskRow existing = tasks.getTask(prepared.taskId());
            if (existing == null && prepared.idempotencyKey() != null) {
                existing = tasks.getTaskByTenantOperatorAndIdempotencyKey(
                        prepared.tenantId(),
                        prepared.operator(),
                        prepared.idempotencyKey()
                );
                if (existing != null) {
                    return new RuntimeTaskUpsertResp(true, existing.taskId(), existing.tenantId(), existing.status().toDb());
                }
            }
            RuntimeTaskServiceSupport.UpsertResolution resolution = RuntimeTaskServiceSupport.applyExisting(existing, prepared);
            if (!resolution.shouldProceed()) {
                return resolution.immediateResponse();
            }
            RuntimeTaskServiceSupport.PreparedRuntimeTaskUpsert effective = resolution.prepared();
            tasks.upsertTask(
                    effective.taskId(),
                    effective.tenantId(),
                    effective.operator(),
                    effective.status(),
                    effective.createdAtEpoch(),
                    effective.updatedAtEpoch(),
                    effective.resultJson(),
                    effective.error(),
                    effective.source(),
                    effective.idempotencyKey(),
                    effective.attempts()
            );
            return new RuntimeTaskUpsertResp(true, effective.taskId(), effective.tenantId(), effective.status().toDb());
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", "runtime task store unavailable");
        }
    }

    public RuntimeTaskGetResp getTask(String taskId) {
        RuntimeTaskRow row;
        try {
            row = tasks.getTask(taskId);
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", "runtime task store unavailable");
        }
        if (row == null) {
            throw ApiException.notFound("task_not_found", "task not found", Map.of("task_id", taskId));
        }
        return new RuntimeTaskGetResp(true, RuntimeTaskServiceSupport.toRuntimeTaskResp(row));
    }

    public RuntimeTaskCancelResp cancelTask(String taskId) {
        long now = System.currentTimeMillis() / 1000L;
        RuntimeTaskCancelResult out;
        try {
            out = tasks.cancelTask(taskId, now);
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", "runtime task store unavailable");
        }
        RuntimeTaskRow task = out.task();
        if (task == null) {
            throw ApiException.notFound("task_not_found", "task not found", Map.of("task_id", taskId));
        }
        return new RuntimeTaskCancelResp(true, taskId, out.cancelled(), task.status().toDb());
    }

    public RuntimeTaskListResp listTasksByTenant(String tenantId, int limit) {
        String effectiveTenantId = RuntimeTaskServiceSupport.defaultIfBlank(tenantId, "default");
        try {
            return new RuntimeTaskListResp(
                    true,
                    effectiveTenantId,
                    tasks.listTasksByTenant(effectiveTenantId, limit).stream().map(RuntimeTaskServiceSupport::toRuntimeTaskResp).toList()
            );
        } catch (DataAccessException e) {
            throw ApiException.serviceUnavailable("runtime_task_store_unavailable", "runtime task store unavailable");
        }
    }
}
