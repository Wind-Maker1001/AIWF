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
                """
                UPDATE dbo.workflow_tasks
                SET tenant_id = ?,
                    operator = ?,
                    status = ?,
                    updated_at_epoch = ?,
                    result_json = ?,
                    error = ?,
                    source = ?,
                    idempotency_key = ?,
                    attempts = ?
                WHERE task_id = ?
                """,
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
                        """
                        INSERT INTO dbo.workflow_tasks
                            (task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
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
                        """
                        UPDATE dbo.workflow_tasks
                        SET tenant_id = ?,
                            operator = ?,
                            status = ?,
                            updated_at_epoch = ?,
                            result_json = ?,
                            error = ?,
                            source = ?,
                            idempotency_key = ?,
                            attempts = ?
                        WHERE task_id = ?
                        """,
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
                """
                SELECT task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
                FROM dbo.workflow_tasks
                WHERE task_id = ?
                """,
                (rs, rowNum) -> new RuntimeTaskRow(
                        rs.getString("task_id"),
                        rs.getString("tenant_id"),
                        rs.getString("operator"),
                        RuntimeTaskStatus.fromDb(rs.getString("status")),
                        rs.getLong("created_at_epoch"),
                        rs.getLong("updated_at_epoch"),
                        rs.getString("result_json"),
                        rs.getString("error"),
                        rs.getString("source"),
                        rs.getString("idempotency_key"),
                        rs.getInt("attempts")
                ),
                taskId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public RuntimeTaskRow getTaskByTenantOperatorAndIdempotencyKey(String tenantId, String operator, String idempotencyKey) {
        try {
            return jdbc.queryForObject(
                """
                SELECT task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
                FROM dbo.workflow_tasks
                WHERE tenant_id = ? AND operator = ? AND idempotency_key = ?
                """,
                (rs, rowNum) -> new RuntimeTaskRow(
                        rs.getString("task_id"),
                        rs.getString("tenant_id"),
                        rs.getString("operator"),
                        RuntimeTaskStatus.fromDb(rs.getString("status")),
                        rs.getLong("created_at_epoch"),
                        rs.getLong("updated_at_epoch"),
                        rs.getString("result_json"),
                        rs.getString("error"),
                        rs.getString("source"),
                        rs.getString("idempotency_key"),
                        rs.getInt("attempts")
                ),
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
                """
                UPDATE dbo.workflow_tasks
                SET status = ?,
                    updated_at_epoch = ?
                WHERE task_id = ? AND status IN (?, ?)
                """,
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
                """
                SELECT TOP (?) task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
                FROM dbo.workflow_tasks
                WHERE tenant_id = ?
                ORDER BY updated_at_epoch DESC
                """,
                (rs, rowNum) -> new RuntimeTaskRow(
                        rs.getString("task_id"),
                        rs.getString("tenant_id"),
                        rs.getString("operator"),
                        RuntimeTaskStatus.fromDb(rs.getString("status")),
                        rs.getLong("created_at_epoch"),
                        rs.getLong("updated_at_epoch"),
                        rs.getString("result_json"),
                        rs.getString("error"),
                        rs.getString("source"),
                        rs.getString("idempotency_key"),
                        rs.getInt("attempts")
                ),
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
