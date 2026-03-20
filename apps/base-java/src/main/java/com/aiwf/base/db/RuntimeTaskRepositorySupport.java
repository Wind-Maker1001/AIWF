package com.aiwf.base.db;

import com.aiwf.base.db.model.RuntimeTaskRow;
import com.aiwf.base.db.model.RuntimeTaskStatus;
import org.springframework.jdbc.core.RowMapper;

final class RuntimeTaskRepositorySupport {
    private RuntimeTaskRepositorySupport() {
    }

    static final String UPDATE_TASK = """
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
            """;

    static final String INSERT_TASK = """
            INSERT INTO dbo.workflow_tasks
                (task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    static final String SELECT_TASK = """
            SELECT task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
            FROM dbo.workflow_tasks
            WHERE task_id = ?
            """;

    static final String SELECT_TASK_BY_IDEMPOTENCY = """
            SELECT task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
            FROM dbo.workflow_tasks
            WHERE tenant_id = ? AND operator = ? AND idempotency_key = ?
            """;

    static final String CANCEL_TASK = """
            UPDATE dbo.workflow_tasks
            SET status = ?,
                updated_at_epoch = ?
            WHERE task_id = ? AND status IN (?, ?)
            """;

    static final String LIST_TASKS_BY_TENANT = """
            SELECT TOP (?) task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source, idempotency_key, attempts
            FROM dbo.workflow_tasks
            WHERE tenant_id = ?
            ORDER BY updated_at_epoch DESC
            """;

    static final RowMapper<RuntimeTaskRow> RUNTIME_TASK_ROW_MAPPER = (rs, rowNum) -> new RuntimeTaskRow(
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
    );
}
