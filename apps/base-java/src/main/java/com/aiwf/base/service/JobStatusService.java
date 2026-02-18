package com.aiwf.base.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class JobStatusService {

    private final JdbcTemplate jdbc;

    public JobStatusService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Step 开始：确保 Job 至少是 RUNNING（CREATED -> RUNNING 幂等） */
    public void onStepStart(String jobId) {
        jdbc.update("""
                UPDATE dbo.jobs
                SET status = 'RUNNING'
                WHERE job_id = ? AND status = 'CREATED'
                """, jobId);
    }

    /** Step 失败：Job 直接 FAILED（幂等，FAILED 不会被后续 DONE 覆盖） */
    public void onStepFail(String jobId) {
        jdbc.update("""
                UPDATE dbo.jobs
                SET status = 'FAILED'
                WHERE job_id = ? AND status <> 'FAILED'
                """, jobId);
    }

    /**
     * Step 完成：重算 Job 状态
     * 规则：
     * - 任一 step FAILED => Job FAILED
     * - 至少有 1 个 step 且全部 DONE => Job DONE
     * - 否则 => Job RUNNING（如果还在 CREATED 也会被推进到 RUNNING）
     */
    public void onStepDone(String jobId) {
        // 如果已失败，不再推进
        Integer jobFailed = jdbc.queryForObject("""
                SELECT COUNT(1) FROM dbo.jobs WHERE job_id=? AND status='FAILED'
                """, Integer.class, jobId);
        if (jobFailed != null && jobFailed > 0) return;

        Integer failed = jdbc.queryForObject("""
                SELECT COUNT(1) FROM dbo.steps WHERE job_id=? AND status='FAILED'
                """, Integer.class, jobId);
        if (failed != null && failed > 0) {
            onStepFail(jobId);
            return;
        }

        Integer total = jdbc.queryForObject("""
                SELECT COUNT(1) FROM dbo.steps WHERE job_id=?
                """, Integer.class, jobId);
        if (total == null || total == 0) return;

        Integer notDone = jdbc.queryForObject("""
                SELECT COUNT(1) FROM dbo.steps WHERE job_id=? AND status<>'DONE'
                """, Integer.class, jobId);

        if (notDone != null && notDone == 0) {
            jdbc.update("""
                    UPDATE dbo.jobs
                    SET status='DONE'
                    WHERE job_id=? AND status<>'FAILED'
                    """, jobId);
        } else {
            // 有未完成 step，确保 RUNNING
            jdbc.update("""
                    UPDATE dbo.jobs
                    SET status='RUNNING'
                    WHERE job_id=? AND status='CREATED'
                    """, jobId);
        }
    }
}
