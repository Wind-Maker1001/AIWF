package com.aiwf.base.db;

import com.aiwf.base.db.model.ArtifactRow;
import com.aiwf.base.db.model.AuditEvent;
import com.aiwf.base.db.model.JobRow;
import com.aiwf.base.db.model.JobStatus;
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
                JobRepositorySupport.INSERT_JOB,
                jobId,
                owner,
                JobStatus.RUNNING.toDb()
        );
        audit(new AuditEvent(jobId, owner, "JOB_CREATE", null, null));
        return jobId;
    }

    public JobRow getJob(String jobId) {
        try {
            return jdbc.queryForObject(
                    JobRepositorySupport.SELECT_JOB,
                    JobRepositorySupport.JOB_ROW_MAPPER,
                    jobId
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<StepRow> listSteps(String jobId) {
        return jdbc.query(
                JobRepositorySupport.SELECT_STEPS,
                JobRepositorySupport.STEP_ROW_MAPPER,
                jobId
        );
    }

    public StepRow getStep(String jobId, String stepId) {
        try {
            return jdbc.queryForObject(
                    JobRepositorySupport.SELECT_STEP,
                    JobRepositorySupport.STEP_ROW_MAPPER,
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
                JobRepositorySupport.UPDATE_STEP_RUNNING,
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
                    JobRepositorySupport.INSERT_STEP_RUNNING,
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
                    JobRepositorySupport.UPDATE_STEP_RUNNING,
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
                JobRepositorySupport.UPDATE_STEP_DONE,
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
                JobRepositorySupport.UPDATE_STEP_FAILED,
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
                JobRepositorySupport.SELECT_ARTIFACTS,
                JobRepositorySupport.ARTIFACT_ROW_MAPPER,
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
                JobRepositorySupport.UPDATE_ARTIFACT,
                kind,
                path,
                sha256,
                jobId,
                artifactId
        );

        completeUpsert(
                updated,
                () -> jdbc.update(
                        JobRepositorySupport.INSERT_ARTIFACT,
                        jobId,
                        artifactId,
                        kind,
                        path,
                        sha256
                ),
                () -> jdbc.update(
                        JobRepositorySupport.UPDATE_ARTIFACT,
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
                JobRepositorySupport.INSERT_AUDIT,
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
