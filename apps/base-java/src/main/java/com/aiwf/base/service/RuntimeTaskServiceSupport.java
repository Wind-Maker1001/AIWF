package com.aiwf.base.service;

import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
import com.aiwf.base.web.ApiException;
import com.aiwf.base.web.dto.RuntimeTaskResp;
import com.aiwf.base.web.dto.RuntimeTaskUpsertReq;
import com.aiwf.base.web.dto.RuntimeTaskUpsertResp;

import java.util.Map;
import java.util.Objects;

final class RuntimeTaskServiceSupport {
    private RuntimeTaskServiceSupport() {
    }

    record PreparedRuntimeTaskUpsert(
            String taskId,
            String tenantId,
            String operator,
            RuntimeTaskStatus status,
            long createdAtEpoch,
            long updatedAtEpoch,
            String resultJson,
            String error,
            String source,
            String idempotencyKey,
            int attempts
    ) {
    }

    record UpsertResolution(
            PreparedRuntimeTaskUpsert prepared,
            RuntimeTaskUpsertResp immediateResponse
    ) {
        static UpsertResolution proceed(PreparedRuntimeTaskUpsert prepared) {
            return new UpsertResolution(prepared, null);
        }

        static UpsertResolution skip(RuntimeTaskRow existing) {
            return new UpsertResolution(
                    null,
                    new RuntimeTaskUpsertResp(true, existing.taskId(), existing.tenantId(), existing.status().toDb())
            );
        }

        boolean shouldProceed() {
            return prepared != null;
        }
    }

    static PreparedRuntimeTaskUpsert prepareUpsert(RuntimeTaskUpsertReq body, String resultJson, long nowEpoch) {
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
            createdAt = updatedAt > 0 ? updatedAt : nowEpoch;
        }
        if (updatedAt <= 0) {
            updatedAt = createdAt;
        }
        return new PreparedRuntimeTaskUpsert(
                taskId,
                tenantId,
                operator,
                status,
                createdAt,
                updatedAt,
                resultJson,
                trimToNull(body.error()),
                defaultIfBlank(body.source(), "accel-rust"),
                idempotencyKey,
                attempts
        );
    }

    static UpsertResolution applyExisting(RuntimeTaskRow existing, PreparedRuntimeTaskUpsert prepared) {
        if (existing == null) {
            return UpsertResolution.proceed(prepared);
        }
        if (!Objects.equals(existing.tenantId(), prepared.tenantId())) {
            throw ApiException.conflict(
                    "runtime_task_tenant_conflict",
                    "runtime task tenant cannot change",
                    Map.of("task_id", prepared.taskId(), "tenant_id", existing.tenantId())
            );
        }
        if (!Objects.equals(existing.operator(), prepared.operator())) {
            throw ApiException.conflict(
                    "runtime_task_operator_conflict",
                    "runtime task operator cannot change",
                    Map.of("task_id", prepared.taskId(), "operator", existing.operator())
            );
        }
        if (existing.idempotencyKey() != null
                && prepared.idempotencyKey() != null
                && !Objects.equals(existing.idempotencyKey(), prepared.idempotencyKey())) {
            throw ApiException.conflict(
                    "runtime_task_idempotency_conflict",
                    "runtime task idempotency key cannot change",
                    Map.of("task_id", prepared.taskId(), "idempotency_key", existing.idempotencyKey())
            );
        }
        if (prepared.updatedAtEpoch() < existing.updatedAtEpoch()) {
            return UpsertResolution.skip(existing);
        }
        if (!existing.status().canTransitionTo(prepared.status())) {
            return UpsertResolution.skip(existing);
        }
        return UpsertResolution.proceed(new PreparedRuntimeTaskUpsert(
                prepared.taskId(),
                prepared.tenantId(),
                prepared.operator(),
                prepared.status(),
                existing.createdAtEpoch(),
                prepared.updatedAtEpoch(),
                prepared.resultJson(),
                prepared.error(),
                prepared.source(),
                prepared.idempotencyKey() == null ? existing.idempotencyKey() : prepared.idempotencyKey(),
                Math.max(existing.attempts(), prepared.attempts())
        ));
    }

    static RuntimeTaskResp toRuntimeTaskResp(RuntimeTaskRow row) {
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

    static RuntimeTaskStatus parseStatus(String value, RuntimeTaskStatus fallback) {
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

    static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    static String defaultIfBlank(String value, String fallback) {
        String trimmed = trimToNull(value);
        return trimmed == null ? fallback : trimmed;
    }
}
