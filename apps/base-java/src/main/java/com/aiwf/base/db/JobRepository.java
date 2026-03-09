package com.aiwf.base.db;

import com.aiwf.base.db.model.ArtifactRow;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.StepRow;
import com.aiwf.base.db.model.StepStatus;
import com.aiwf.base.db.model.StepTransitionResult;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public class JobRepository {

    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public String createJob(String owner) {
        String jobId = UUID.randomUUID().toString().replace("-", "");
        jdbc.update(
                """
                INSERT INTO dbo.jobs (job_id, owner, status, created_at)
                VALUES (?, ?, ?, SYSDATETIME())
                """,
                jobId,
                owner,
                "RUNNING"
        );
        audit(new AuditEvent(jobId, owner, "JOB_CREATE", null, null));
        return jobId;
    }

    public JobRow getJob(String jobId) {
        try {
            return jdbc.queryForObject(
                    "SELECT job_id, owner, status, created_at FROM dbo.jobs WHERE job_id = ?",
                    (rs, rowNum) -> new JobRow(
                            rs.getString("job_id"),
                            rs.getObject("created_at"),
                            rs.getString("owner"),
                            rs.getString("status")
                    ),
                    jobId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<StepRow> listSteps(String jobId) {
        return jdbc.query(
                """
                SELECT job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json,
                       started_at, ended_at, output_hash, error
                FROM dbo.steps
                WHERE job_id = ?
                ORDER BY started_at ASC
                """,
                (rs, rowNum) -> new StepRow(
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
                ),
                jobId
        );
    }

    public StepRow getStep(String jobId, String stepId) {
        try {
            return jdbc.queryForObject(
                    """
                    SELECT job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json,
                           started_at, ended_at, output_hash, error
                    FROM dbo.steps
                    WHERE job_id = ? AND step_id = ?
                    """,
                    (rs, rowNum) -> new StepRow(
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
                    ),
                    jobId,
                    stepId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public StepTransitionResult upsertStepRunning(
            String jobId,
            String stepId,
            String inputUri,
            String outputUri,
            String rulesetVersion,
            String paramsJson
    ) {
        String rv = safeRulesetVersion(rulesetVersion);

        int updated = jdbc.update(
                """
                UPDATE dbo.steps
                SET status = ?,
                    input_uri = ?,
                    output_uri = ?,
                    ruleset_version = ?,
                    params_json = ?,
                    started_at = COALESCE(started_at, SYSDATETIME())
                WHERE job_id = ? AND step_id = ? AND status = ?
                """,
                StepStatus.RUNNING.toDb(),
                inputUri,
                outputUri,
                rv,
                paramsJson,
                jobId,
                stepId,
                StepStatus.RUNNING.toDb()
        );

        if (updated > 0) {
            return new StepTransitionResult(getStep(jobId, stepId), true);
        }

        try {
            jdbc.update(
                    """
                    INSERT INTO dbo.steps
                        (job_id, step_id, status, input_uri, output_uri, ruleset_version, params_json, started_at)
                    VALUES
                        (?, ?, ?, ?, ?, ?, ?, SYSDATETIME())
                    """,
                    jobId,
                    stepId,
                    StepStatus.RUNNING.toDb(),
                    inputUri,
                    outputUri,
                    rv,
                    paramsJson
            );
            return new StepTransitionResult(getStep(jobId, stepId), true);
        } catch (DuplicateKeyException e) {
            int retried = jdbc.update(
                    """
                    UPDATE dbo.steps
                    SET status = ?,
                        input_uri = ?,
                        output_uri = ?,
                        ruleset_version = ?,
                        params_json = ?,
                        started_at = COALESCE(started_at, SYSDATETIME())
                    WHERE job_id = ? AND step_id = ? AND status = ?
                    """,
                    StepStatus.RUNNING.toDb(),
                    inputUri,
                    outputUri,
                    rv,
                    paramsJson,
                    jobId,
                    stepId,
                    StepStatus.RUNNING.toDb()
            );
            return new StepTransitionResult(getStep(jobId, stepId), retried > 0);
        }
    }

    public StepTransitionResult markStepDone(String jobId, String stepId, String outputHash) {
        int updated = jdbc.update(
                """
                UPDATE dbo.steps
                SET status = ?,
                    output_hash = ?,
                    ended_at = SYSDATETIME()
                WHERE job_id = ? AND step_id = ? AND status = ?
                """,
                StepStatus.DONE.toDb(),
                outputHash,
                jobId,
                stepId,
                StepStatus.RUNNING.toDb()
        );
        return new StepTransitionResult(getStep(jobId, stepId), updated > 0);
    }

    public StepTransitionResult markStepFailed(String jobId, String stepId, String error) {
        int updated = jdbc.update(
                """
                UPDATE dbo.steps
                SET status = ?,
                    error = ?,
                    ended_at = SYSDATETIME()
                WHERE job_id = ? AND step_id = ? AND status = ?
                """,
                StepStatus.FAILED.toDb(),
                error,
                jobId,
                stepId,
                StepStatus.RUNNING.toDb()
        );
        return new StepTransitionResult(getStep(jobId, stepId), updated > 0);
    }

    public List<ArtifactRow> listArtifacts(String jobId) {
        return jdbc.query(
                """
                SELECT artifact_id, kind, path, sha256, created_at
                FROM dbo.artifacts
                WHERE job_id = ?
                ORDER BY created_at DESC
                """,
                (rs, rowNum) -> new ArtifactRow(
                        rs.getString("artifact_id"),
                        rs.getString("kind"),
                        rs.getString("path"),
                        rs.getString("sha256"),
                        rs.getObject("created_at")
                ),
                jobId
        );
    }

    public void upsertArtifact(
            String jobId,
            String artifactId,
            String kind,
            String path,
            String sha256
    ) {
        int updated = jdbc.update(
                """
                UPDATE dbo.artifacts
                SET kind = ?,
                    path = ?,
                    sha256 = ?
                WHERE job_id = ? AND artifact_id = ?
                """,
                kind,
                path,
                sha256,
                jobId,
                artifactId
        );

        completeUpsert(
                updated,
                () -> jdbc.update(
                        """
                        INSERT INTO dbo.artifacts (job_id, artifact_id, kind, path, sha256, created_at)
                        VALUES (?, ?, ?, ?, ?, SYSDATETIME())
                        """,
                        jobId,
                        artifactId,
                        kind,
                        path,
                        sha256
                ),
                () -> jdbc.update(
                        """
                        UPDATE dbo.artifacts
                        SET kind = ?,
                            path = ?,
                            sha256 = ?
                        WHERE job_id = ? AND artifact_id = ?
                        """,
                        kind,
                        path,
                        sha256,
                        jobId,
                        artifactId
                )
        );
    }

    public void audit(AuditEvent event) {
        jdbc.update(
                """
                INSERT INTO dbo.audit_log (job_id, actor, action, step_id, detail_json, created_at)
                VALUES (?, ?, ?, ?, ?, SYSDATETIME())
                """,
                event.jobId(),
                event.actor(),
                event.action(),
                event.stepId(),
                event.detailJson()
        );
    }

    private String safeRulesetVersion(String rulesetVersion) {
        if (rulesetVersion == null) {
            return "v1";
        }
        String rv = rulesetVersion.trim();
        if (rv.isEmpty()) {
            return "v1";
        }
        if (rv.length() > 32) {
            rv = rv.substring(0, 32);
        }
        return rv;
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
