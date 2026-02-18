-- AIWF Control Plane DB init (run with sqlcmd or SSMS)
-- Creates database and minimal schema placeholders.

IF DB_ID(N'AIWF') IS NULL
BEGIN
  CREATE DATABASE [AIWF];
END
GO

USE [AIWF];
GO

-- Jobs
IF OBJECT_ID('dbo.jobs', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.jobs (
    job_id            NVARCHAR(64)  NOT NULL PRIMARY KEY,
    created_at        DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    owner             NVARCHAR(128)  NULL,
    status            NVARCHAR(32)   NOT NULL,
    policy_json       NVARCHAR(MAX)  NULL,
    root_path         NVARCHAR(512)  NULL,
    bus_path          NVARCHAR(512)  NULL,
    lake_path         NVARCHAR(512)  NULL
  );
END
GO

-- Steps
IF OBJECT_ID('dbo.steps', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.steps (
    job_id            NVARCHAR(64)  NOT NULL,
    step_id           NVARCHAR(64)  NOT NULL,
    created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
    status            NVARCHAR(32)  NOT NULL,
    input_uri         NVARCHAR(1024) NULL,
    output_uri        NVARCHAR(1024) NULL,
    params_json       NVARCHAR(MAX) NULL,
    error_summary     NVARCHAR(MAX) NULL,
    PRIMARY KEY (job_id, step_id),
    CONSTRAINT FK_steps_jobs FOREIGN KEY (job_id) REFERENCES dbo.jobs(job_id)
  );
END
GO

-- Artifacts
IF OBJECT_ID('dbo.artifacts', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.artifacts (
    job_id           NVARCHAR(64)   NOT NULL,
    artifact_id      NVARCHAR(64)   NOT NULL,
    kind             NVARCHAR(32)   NOT NULL, -- docx/pptx/xlsx/powerbi/...
    path             NVARCHAR(1024) NOT NULL,
    sha256           NVARCHAR(128)  NULL,
    binding_json     NVARCHAR(MAX)  NULL,
    created_at       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    PRIMARY KEY (job_id, artifact_id),
    CONSTRAINT FK_artifacts_jobs FOREIGN KEY (job_id) REFERENCES dbo.jobs(job_id)
  );
END
GO