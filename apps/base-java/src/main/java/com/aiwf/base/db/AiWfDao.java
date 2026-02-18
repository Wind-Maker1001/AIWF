package com.aiwf.base.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

@Repository
public class AiWfDao {

    private final JdbcTemplate jdbc;
    private final ObjectMapper om;
    private final ColumnMapRowMapper rowMapper = new ColumnMapRowMapper();

    public AiWfDao(JdbcTemplate jdbc, ObjectMapper om) {
        this.jdbc = jdbc;
        this.om = om;
    }

    // -------------------------
    // Jobs
    // -------------------------

    public String newJobId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 创建 job（注意：不再写 bus_job_path 这种不存在的列）
     */
    public String createJob(String owner) {
        String jobId = newJobId();
        insertJob(jobId, owner, "RUNNING");
        // 兼容：stepId 允许为 null
        audit(jobId, owner, "JOB_CREATE", null);
        return jobId;
    }

    private void insertJob(String jobId, String owner, String status) {
        // 只写已确认存在的列：job_id/owner/status/created_at
        // 如果你表里还有别的可空列（如 updated_at），不影响
        String sql = """
                INSERT INTO dbo.jobs (job_id, owner, status, created_at)
                VALUES (?, ?, ?, SYSDATETIME())
                """;
        jdbc.update(sql, jobId, owner, status);
    }

    public Map<String, Object> getJob(String jobId) {
        try {
            return jdbc.queryForObject(
                    "SELECT job_id, owner, status, created_at FROM dbo.jobs WHERE job_id = ?",
                    rowMapper,
                    jobId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public void updateJobStatus(String jobId, String status) {
        jdbc.update("UPDATE dbo.jobs SET status = ? WHERE job_id = ?", status, jobId);
    }

    // -------------------------
    // Steps
    // -------------------------

    public List<Map<String, Object>> listSteps(String jobId) {
        return jdbc.query(
                """
                SELECT job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json,
                       started_at, ended_at, output_hash, error
                FROM dbo.steps
                WHERE job_id = ?
                ORDER BY started_at ASC
                """,
                rowMapper,
                jobId
        );
    }

    /**
     * 兼容旧命名：CallbackController 里常用 upsertStepRunning
     */
    public void upsertStepRunning(
            String jobId,
            String stepId,
            String inputUri,
            String outputUri,
            String rulesetVersion,
            String paramsJson
    ) {
        String rv = safeRulesetVersion(rulesetVersion);

        // 先 UPDATE，若不存在再 INSERT（避免 MERGE 的坑）
        int updated = jdbc.update(
                """
                UPDATE dbo.steps
                SET status = 'RUNNING',
                    input_uri = ?,
                    output_uri = ?,
                    ruleset_version = ?,
                    params_json = ?,
                    started_at = COALESCE(started_at, SYSDATETIME())
                WHERE job_id = ? AND step_id = ?
                """,
                inputUri,
                outputUri,
                rv,          // ✅ ruleset_version 放这里
                paramsJson,  // ✅ params_json 放这里（别再塞反了）
                jobId,
                stepId
        );

        if (updated == 0) {
            jdbc.update(
                    """
                    INSERT INTO dbo.steps
                        (job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json, started_at)
                    VALUES
                        (?, ?, 'RUNNING', ?, ?, ?, ?, SYSDATETIME())
                    """,
                    jobId,
                    stepId,
                    inputUri,
                    outputUri,
                    rv,
                    paramsJson
            );
        }
    }

    /**
     * 如果你的 CallbackController 用的是 upsertStepStart 这个名字，也给一个别名
     */
    public void upsertStepStart(
            String jobId,
            String stepId,
            String inputUri,
            String outputUri,
            String rulesetVersion,
            String paramsJson
    ) {
        upsertStepRunning(jobId, stepId, inputUri, outputUri, rulesetVersion, paramsJson);
    }

    public void markStepDone(String jobId, String stepId, String outputHash) {
        jdbc.update(
                """
                UPDATE dbo.steps
                SET status = 'DONE',
                    output_hash = ?,
                    ended_at = SYSDATETIME()
                WHERE job_id = ? AND step_id = ?
                """,
                outputHash, jobId, stepId
        );
    }

    public void markStepFailed(String jobId, String stepId, String error) {
        jdbc.update(
                """
                UPDATE dbo.steps
                SET status = 'FAILED',
                    error = ?,
                    ended_at = SYSDATETIME()
                WHERE job_id = ? AND step_id = ?
                """,
                error, jobId, stepId
        );
    }

    // -------------------------
    // Artifacts
    // -------------------------

    public List<Map<String, Object>> listArtifacts(String jobId) {
        return jdbc.query(
                """
                SELECT artifact_id, kind, path, sha256, created_at
                FROM dbo.artifacts
                WHERE job_id = ?
                ORDER BY created_at DESC
                """,
                rowMapper,
                jobId
        );
    }

    public void upsertArtifact(
            String jobId,
            String artifactId,
            String kind,
            String path,
            String sha256,
            String extraJson
    ) {
        // extraJson 目前不强依赖（你表里如果有 meta_json 再加）
        int updated = jdbc.update(
                """
                UPDATE dbo.artifacts
                SET kind = ?,
                    path = ?,
                    sha256 = ?
                WHERE job_id = ? AND artifact_id = ?
                """,
                kind, path, sha256, jobId, artifactId
        );

        if (updated == 0) {
            jdbc.update(
                    """
                    INSERT INTO dbo.artifacts (job_id, artifact_id, kind, path, sha256, created_at)
                    VALUES (?, ?, ?, ?, ?, SYSDATETIME())
                    """,
                    jobId, artifactId, kind, path, sha256
            );
        }
    }

    // -------------------------
    // Audit log
    // -------------------------

    /**
     * 重要：Controller 里历史上存在两种参数顺序：
     *  1) 新顺序：audit(jobId, actor, action, stepId[, msg])
     *  2) 旧顺序：audit(actor, action, jobId, stepId[, msg])
     *
     * 由于这两种写法“全是 String”，Java 不能靠重载区分（签名一样）。
     * 所以这里统一用 varargs 接住，再通过 jobId 的形态（32位hex）识别位置。
     */
    public void audit(String... args) {
        if (args == null || args.length < 4) return;

        String jobId;
        String actor;
        String action;
        String stepId;

        // 优先识别 jobId 在 args[0]（新顺序）或 args[2]（旧顺序）
        if (isJobId(args[0])) {
            // audit(jobId, actor, action, stepId[, msg])
            jobId = args[0];
            actor = args[1];
            action = args[2];
            stepId = args[3];
        } else if (isJobId(args[2])) {
            // audit(actor, action, jobId, stepId[, msg])
            actor = args[0];
            action = args[1];
            jobId = args[2];
            stepId = args[3];
        } else {
            // 实在识别不了就按“新顺序”兜底，至少别把 action/actor 写反
            jobId = args[0];
            actor = args[1];
            action = args[2];
            stepId = args[3];
        }

        // 最小列集落库，避免你表没有 msg/detail 列导致 500
        auditInsert(jobId, actor, action, stepId);

        // 如果你未来 audit_log 表加了 message/detail 列，可以在这里利用 args[4]
        // String msg = (args.length >= 5 ? args[4] : null);
    }

    private void auditInsert(String jobId, String actor, String action, String stepId) {
        // ✅ 必须按 job_id, actor, action, step_id 的顺序写入
        jdbc.update(
                """
                INSERT INTO dbo.audit_log (job_id, actor, action, step_id, created_at)
                VALUES (?, ?, ?, ?, SYSDATETIME())
                """,
                jobId, actor, action, stepId
        );
    }

    private boolean isJobId(String s) {
        if (s == null) return false;
        // 你的 jobId 是 UUID 去掉 "-"，即 32 位十六进制
        if (s.length() != 32) return false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean ok =
                    (c >= '0' && c <= '9') ||
                    (c >= 'a' && c <= 'f') ||
                    (c >= 'A' && c <= 'F');
            if (!ok) return false;
        }
        return true;
    }

    // -------------------------
    // Helpers
    // -------------------------

    public void upsertWorkflowTask(
            String taskId,
            String tenantId,
            String operator,
            String status,
            long createdAtEpoch,
            long updatedAtEpoch,
            String resultJson,
            String error,
            String source
    ) {
        int updated = jdbc.update(
                """
                UPDATE dbo.workflow_tasks
                SET tenant_id = ?,
                    operator = ?,
                    status = ?,
                    created_at_epoch = ?,
                    updated_at_epoch = ?,
                    result_json = ?,
                    error = ?,
                    source = ?
                WHERE task_id = ?
                """,
                tenantId, operator, status, createdAtEpoch, updatedAtEpoch, resultJson, error, source, taskId
        );
        if (updated == 0) {
            jdbc.update(
                    """
                    INSERT INTO dbo.workflow_tasks
                        (task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    taskId, tenantId, operator, status, createdAtEpoch, updatedAtEpoch, resultJson, error, source
            );
        }
    }

    public Map<String, Object> getWorkflowTask(String taskId) {
        try {
            return jdbc.queryForObject(
                    """
                    SELECT task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, result_json, error, source
                    FROM dbo.workflow_tasks
                    WHERE task_id = ?
                    """,
                    rowMapper,
                    taskId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public Map<String, Object> cancelWorkflowTask(String taskId, long updatedAtEpoch) {
        int changed = jdbc.update(
                """
                UPDATE dbo.workflow_tasks
                SET status = 'cancelled',
                    updated_at_epoch = ?
                WHERE task_id = ? AND status IN ('queued', 'running')
                """,
                updatedAtEpoch, taskId
        );
        Map<String, Object> row = getWorkflowTask(taskId);
        Map<String, Object> out = new HashMap<>();
        out.put("cancelled", changed > 0);
        out.put("task", row);
        return out;
    }

    public List<Map<String, Object>> listWorkflowTasksByTenant(String tenantId, int limit) {
        int n = Math.max(1, Math.min(500, limit));
        return jdbc.query(
                """
                SELECT TOP (?) task_id, tenant_id, operator, status, created_at_epoch, updated_at_epoch, source
                FROM dbo.workflow_tasks
                WHERE tenant_id = ?
                ORDER BY updated_at_epoch DESC
                """,
                rowMapper,
                n, tenantId
        );
    }

    public String toJsonOrNull(Object obj) {
        if (obj == null) return null;
        if (obj instanceof String s) return s; // 已经是 json 字符串就直接用
        try {
            return om.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            return String.valueOf(obj);
        }
    }

    private String safeRulesetVersion(String rulesetVersion) {
        if (rulesetVersion == null) return "v1";
        String rv = rulesetVersion.trim();
        if (rv.isEmpty()) return "v1";
        // 防御：避免列太短导致再次截断（你可以按真实字段长度改成 16/32）
        if (rv.length() > 32) rv = rv.substring(0, 32);
        return rv;
    }
}
