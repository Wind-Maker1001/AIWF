IF OBJECT_ID('dbo.jobs', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.jobs (
        job_id NVARCHAR(64) NOT NULL PRIMARY KEY,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        owner NVARCHAR(128) NULL,
        status NVARCHAR(32) NOT NULL,
        policy_json NVARCHAR(MAX) NULL,
        root_path NVARCHAR(512) NULL,
        bus_path NVARCHAR(512) NULL,
        lake_path NVARCHAR(512) NULL
    )
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ux_workflow_tasks_tenant_idempotency'
      AND object_id = OBJECT_ID(N'dbo.workflow_tasks')
)
BEGIN
    CREATE UNIQUE INDEX ux_workflow_tasks_tenant_idempotency
        ON dbo.workflow_tasks (tenant_id, operator, idempotency_key)
        WHERE idempotency_key IS NOT NULL
END;

IF OBJECT_ID('dbo.steps', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.steps (
        job_id NVARCHAR(64) NOT NULL,
        step_id NVARCHAR(64) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        status NVARCHAR(32) NOT NULL,
        input_uri NVARCHAR(1024) NULL,
        output_uri NVARCHAR(1024) NULL,
        params_json NVARCHAR(MAX) NULL,
        error_summary NVARCHAR(MAX) NULL,
        ruleset_version NVARCHAR(64) NULL,
        started_at DATETIME2 NULL,
        ended_at DATETIME2 NULL,
        output_hash NVARCHAR(128) NULL,
        error NVARCHAR(1024) NULL,
        PRIMARY KEY (job_id, step_id),
        CONSTRAINT FK_steps_jobs FOREIGN KEY (job_id) REFERENCES dbo.jobs(job_id)
    )
END;

IF OBJECT_ID('dbo.artifacts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.artifacts (
        job_id NVARCHAR(64) NOT NULL,
        artifact_id NVARCHAR(64) NOT NULL,
        kind NVARCHAR(32) NOT NULL,
        path NVARCHAR(1024) NOT NULL,
        sha256 NVARCHAR(128) NULL,
        binding_json NVARCHAR(MAX) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        PRIMARY KEY (job_id, artifact_id),
        CONSTRAINT FK_artifacts_jobs FOREIGN KEY (job_id) REFERENCES dbo.jobs(job_id)
    )
END;

IF OBJECT_ID('dbo.audit_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.audit_log (
        audit_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        actor NVARCHAR(128) NULL,
        action NVARCHAR(128) NOT NULL,
        job_id NVARCHAR(64) NULL,
        step_id NVARCHAR(64) NULL,
        detail_json NVARCHAR(MAX) NULL
    )
END;

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.workflow_tasks (
        task_id NVARCHAR(64) NOT NULL PRIMARY KEY,
        tenant_id NVARCHAR(64) NOT NULL DEFAULT N'default',
        operator NVARCHAR(64) NOT NULL,
        status NVARCHAR(32) NOT NULL,
        created_at_epoch BIGINT NOT NULL,
        updated_at_epoch BIGINT NOT NULL,
        result_json NVARCHAR(MAX) NULL,
        error NVARCHAR(MAX) NULL,
        source NVARCHAR(32) NOT NULL DEFAULT N'accel-rust',
        idempotency_key NVARCHAR(128) NULL,
        attempts INT NOT NULL DEFAULT 0
    )
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_steps_status' AND object_id = OBJECT_ID('dbo.steps'))
BEGIN
    CREATE INDEX IX_steps_status ON dbo.steps(status)
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_jobs_status' AND object_id = OBJECT_ID('dbo.jobs'))
BEGIN
    CREATE INDEX IX_jobs_status ON dbo.jobs(status)
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_workflow_tasks_status' AND object_id = OBJECT_ID('dbo.workflow_tasks'))
BEGIN
    CREATE INDEX IX_workflow_tasks_status ON dbo.workflow_tasks(status)
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_workflow_tasks_updated' AND object_id = OBJECT_ID('dbo.workflow_tasks'))
BEGIN
    CREATE INDEX IX_workflow_tasks_updated ON dbo.workflow_tasks(updated_at_epoch DESC)
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_workflow_tasks_tenant_status' AND object_id = OBJECT_ID('dbo.workflow_tasks'))
BEGIN
    CREATE INDEX IX_workflow_tasks_tenant_status ON dbo.workflow_tasks(tenant_id, status)
END;
