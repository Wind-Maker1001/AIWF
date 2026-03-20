package com.aiwf.base.db;

import com.aiwf.base.db.model.RuntimeTaskCancelResult;
import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class RuntimeTaskRepository {

    private final JdbcTemplate jdbc;

    public RuntimeTaskRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void upsertTask(
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
        int updated = jdbc.update(
                RuntimeTaskRepositorySupport.UPDATE_TASK,
                tenantId,
                operator,
                status.toDb(),
                updatedAtEpoch,
                resultJson,
                error,
                source,
                idempotencyKey,
                attempts,
                taskId
        );

        completeUpsert(
                updated,
                () -> jdbc.update(
                        RuntimeTaskRepositorySupport.INSERT_TASK,
                        taskId,
                        tenantId,
                        operator,
                        status.toDb(),
                        createdAtEpoch,
                        updatedAtEpoch,
                        resultJson,
                        error,
                        source,
                        idempotencyKey,
                        attempts
                ),
                () -> jdbc.update(
                        RuntimeTaskRepositorySupport.UPDATE_TASK,
                        tenantId,
                        operator,
                        status.toDb(),
                        updatedAtEpoch,
                        resultJson,
                        error,
                        source,
                        idempotencyKey,
                        attempts,
                        taskId
                )
        );
    }

    public RuntimeTaskRow getTask(String taskId) {
        try {
            return jdbc.queryForObject(
                RuntimeTaskRepositorySupport.SELECT_TASK,
                RuntimeTaskRepositorySupport.RUNTIME_TASK_ROW_MAPPER,
                taskId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public RuntimeTaskRow getTaskByTenantOperatorAndIdempotencyKey(String tenantId, String operator, String idempotencyKey) {
        try {
            return jdbc.queryForObject(
                RuntimeTaskRepositorySupport.SELECT_TASK_BY_IDEMPOTENCY,
                RuntimeTaskRepositorySupport.RUNTIME_TASK_ROW_MAPPER,
                tenantId,
                operator,
                idempotencyKey
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public RuntimeTaskCancelResult cancelTask(String taskId, long updatedAtEpoch) {
        int changed = jdbc.update(
                RuntimeTaskRepositorySupport.CANCEL_TASK,
                RuntimeTaskStatus.CANCELLED.toDb(),
                updatedAtEpoch,
                taskId,
                RuntimeTaskStatus.QUEUED.toDb(),
                RuntimeTaskStatus.RUNNING.toDb()
        );
        return new RuntimeTaskCancelResult(changed > 0, getTask(taskId));
    }

    public List<RuntimeTaskRow> listTasksByTenant(String tenantId, int limit) {
        int n = Math.max(1, Math.min(500, limit));
        return jdbc.query(
                RuntimeTaskRepositorySupport.LIST_TASKS_BY_TENANT,
                RuntimeTaskRepositorySupport.RUNTIME_TASK_ROW_MAPPER,
                n,
                tenantId
        );
    }

    private void completeUpsert(int updated, Runnable insertAction, Runnable retryUpdateAction) {
        if (updated > 0) {
            return;
        }
        try {
            insertAction.run();
        } catch (DuplicateKeyException e) {
            retryUpdateAction.run();
        }
    }
}
