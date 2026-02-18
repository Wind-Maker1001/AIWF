-- LEGACY NOTICE: This script is kept for historical compatibility.
-- Canonical entrypoint is ops/scripts/db_migrate.ps1.
USE [AIWF];
GO

-- datasets
IF OBJECT_ID('dbo.datasets','U') IS NULL
BEGIN
  CREATE TABLE dbo.datasets (
    dataset_id        NVARCHAR(64)   NOT NULL PRIMARY KEY,
    job_id            NVARCHAR(64)   NOT NULL,
    created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    snapshot_path     NVARCHAR(1024) NOT NULL,
    schema_hash       NVARCHAR(128)  NULL,
    quality_json      NVARCHAR(MAX)  NULL,
    lineage_json      NVARCHAR(MAX)  NULL,
    CONSTRAINT FK_datasets_jobs FOREIGN KEY (job_id) REFERENCES dbo.jobs(job_id)
  );
END
GO

-- rulesets
IF OBJECT_ID('dbo.rulesets','U') IS NULL
BEGIN
  CREATE TABLE dbo.rulesets (
    ruleset_version   NVARCHAR(64)   NOT NULL PRIMARY KEY,
    created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    description       NVARCHAR(512)  NULL,
    content_hash      NVARCHAR(128)  NULL,
    path              NVARCHAR(1024) NULL
  );
END
GO

-- llm_calls (閸欘亝鏂侀懘杈ㄦ櫛閸氬海娈?meta閿涘苯鍩嗛弨鎯у斧婵妲戠紒?
IF OBJECT_ID('dbo.llm_calls','U') IS NULL
BEGIN
  CREATE TABLE dbo.llm_calls (
    call_id            NVARCHAR(64)   NOT NULL PRIMARY KEY,
    job_id             NVARCHAR(64)   NULL,
    step_id            NVARCHAR(64)   NULL,
    created_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    provider           NVARCHAR(64)   NOT NULL,
    model              NVARCHAR(128)  NULL,
    prompt_hash        NVARCHAR(128)  NULL,
    request_meta_json  NVARCHAR(MAX)  NULL,
    response_meta_json NVARCHAR(MAX)  NULL,
    tokens_in          INT            NULL,
    tokens_out         INT            NULL
  );
END
GO

-- audit_log
IF OBJECT_ID('dbo.audit_log','U') IS NULL
BEGIN
  CREATE TABLE dbo.audit_log (
    audit_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    created_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    actor        NVARCHAR(128) NULL,
    action       NVARCHAR(128) NOT NULL,
    job_id       NVARCHAR(64)  NULL,
    step_id      NVARCHAR(64)  NULL,
    detail_json  NVARCHAR(MAX) NULL
  );
END
GO

-- indexes
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_steps_status' AND object_id=OBJECT_ID('dbo.steps'))
  CREATE INDEX IX_steps_status ON dbo.steps(status);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_jobs_status' AND object_id=OBJECT_ID('dbo.jobs'))
  CREATE INDEX IX_jobs_status ON dbo.jobs(status);
GO
