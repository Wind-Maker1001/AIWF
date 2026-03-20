package com.aiwf.base.db;

import com.aiwf.base.db.model.ArtifactRow;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.JobStatus;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepStatus;
import org.springframework.jdbc.core.RowMapper;

final class JobRepositorySupport {
    private JobRepositorySupport() {
    }

    static final String INSERT_JOB = """
            INSERT INTO dbo.jobs (job_id, owner, status, created_at)
            VALUES (?, ?, ?, SYSDATETIME())
            """;

    static final String SELECT_JOB = "SELECT job_id, owner, status, created_at FROM dbo.jobs WHERE job_id = ?";

    static final String SELECT_STEPS = """
            SELECT job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json,
                   started_at, ended_at, output_hash, error
            FROM dbo.steps
            WHERE job_id = ?
            ORDER BY started_at ASC
            """;

    static final String SELECT_STEP = """
            SELECT job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json,
                   started_at, ended_at, output_hash, error
            FROM dbo.steps
            WHERE job_id = ? AND step_id = ?
            """;

    static final String UPDATE_STEP_RUNNING = """
            UPDATE dbo.steps
            SET status = ?,
                input_uri = ?,
                output_uri = ?,
                ruleset_version = ?,
                params_json = ?,
                started_at = COALESCE(started_at, SYSDATETIME())
            WHERE job_id = ? AND step_id = ? AND status = ?
            """;

    static final String INSERT_STEP_RUNNING = """
            INSERT INTO dbo.steps
                (job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json, started_at)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, SYSDATETIME())
            """;

    static final String UPDATE_STEP_DONE = """
            UPDATE dbo.steps
            SET status = ?,
                output_hash = ?,
                ended_at = SYSDATETIME()
            WHERE job_id = ? AND step_id = ? AND status = ?
            """;

    static final String UPDATE_STEP_FAILED = """
            UPDATE dbo.steps
            SET status = ?,
                error = ?,
                ended_at = SYSDATETIME()
            WHERE job_id = ? AND step_id = ? AND status = ?
            """;

    static final String SELECT_ARTIFACTS = """
            SELECT artifact_id, kind, path, sha256, created_at
            FROM dbo.artifacts
            WHERE job_id = ?
            ORDER BY created_at DESC
            """;

    static final String UPDATE_ARTIFACT = """
            UPDATE dbo.artifacts
            SET kind = ?,
                path = ?,
                sha256 = ?
            WHERE job_id = ? AND artifact_id = ?
            """;

    static final String INSERT_ARTIFACT = """
            INSERT INTO dbo.artifacts (job_id, artifact_id, kind, path, sha256, created_at)
            VALUES (?, ?, ?, ?, ?, SYSDATETIME())
            """;

    static final String INSERT_AUDIT = """
            INSERT INTO dbo.audit_log (job_id, actor, action, step_id, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?, SYSDATETIME())
            """;

    static final RowMapper<JobRow> JOB_ROW_MAPPER = (rs, rowNum) -> new JobRow(
            rs.getString("job_id"),
            rs.getObject("created_at"),
            rs.getString("owner"),
            JobStatus.fromDb(rs.getString("status"))
    );

    static final RowMapper<StepRow> STEP_ROW_MAPPER = (rs, rowNum) -> new StepRow(
            rs.getString("job_id"),
            rs.getString("step_id"),
            StepStatus.fromDb(rs.getString("status")),
            rs.getString("input_uri"),
            rs.getString("output_uri"),
            rs.getString("ruleset_version"),
            rs.getString("params_json"),
            rs.getObject("started_at"),
            rs.getObject("ended_at"),
            rs.getString("output_hash"),
            rs.getString("error")
    );

    static final RowMapper<ArtifactRow> ARTIFACT_ROW_MAPPER = (rs, rowNum) -> new ArtifactRow(
            rs.getString("artifact_id"),
            rs.getString("kind"),
            rs.getString("path"),
            rs.getString("sha256"),
            rs.getObject("created_at")
    );
}
