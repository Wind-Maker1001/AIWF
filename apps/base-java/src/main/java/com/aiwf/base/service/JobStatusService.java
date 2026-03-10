package com.aiwf.base.service;

import com.aiwf.base.db.model.JobStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JobStatusService {

    private final JdbcTemplate jdbc;

    public JobStatusService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Keep jobs moving into RUNNING whenever work resumes, unless already failed. */
    public void onStepStart(String jobId) {
        jdbc.update("""
                UPDATE dbo.jobs
                SET status = ?
                WHERE job_id = ? AND status <> ?
                """, JobStatus.RUNNING.toDb(), jobId, JobStatus.FAILED.toDb());
    }

    /** A single failed step makes the job failed and keeps it there. */
    public void onStepFail(String jobId) {
        jdbc.update("""
                UPDATE dbo.jobs
                SET status = ?
                WHERE job_id = ? AND status <> ?
                """, JobStatus.FAILED.toDb(), jobId, JobStatus.FAILED.toDb());
    }

    /**
     * Recompute the job status from the current step set.
     * FAILED wins, all DONE means DONE, otherwise the job remains RUNNING.
     */
    public void onStepDone(String jobId) {
        StepCounts counts = jdbc.queryForObject(
                """
                SELECT COUNT(1) AS total_steps,
                       SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_steps,
                       SUM(CASE WHEN status <> 'DONE' THEN 1 ELSE 0 END) AS not_done_steps
                FROM dbo.steps
                WHERE job_id = ?
                """,
                (rs, rowNum) -> new StepCounts(
                        rs.getInt("total_steps"),
                        rs.getInt("failed_steps"),
                        rs.getInt("not_done_steps")
                ),
                jobId
        );

        if (counts == null || counts.total() == 0) {
            return;
        }

        if (counts.failed() > 0) {
            onStepFail(jobId);
            return;
        }

        if (counts.notDone() == 0) {
            jdbc.update("""
                    UPDATE dbo.jobs
                    SET status=?
                    WHERE job_id=? AND status<>?
                    """, JobStatus.DONE.toDb(), jobId, JobStatus.FAILED.toDb());
        } else {
            jdbc.update("""
                    UPDATE dbo.jobs
                    SET status=?
                    WHERE job_id=? AND status NOT IN (?, ?)
                    """, JobStatus.RUNNING.toDb(), jobId, JobStatus.FAILED.toDb(), JobStatus.RUNNING.toDb());
        }
    }

    private record StepCounts(int total, int failed, int notDone) {}
}
