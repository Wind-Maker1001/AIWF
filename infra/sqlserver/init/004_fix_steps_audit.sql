-- LEGACY NOTICE: This script is kept for historical compatibility.
-- Canonical entrypoint is ops/scripts/db_migrate.ps1.
USE AIWF;
GO

-- ===== steps: ensure columns exist =====
IF OBJECT_ID('dbo.steps','U') IS NOT NULL
BEGIN
  IF COL_LENGTH('dbo.steps','input_uri') IS NULL
    ALTER TABLE dbo.steps ADD input_uri NVARCHAR(1024) NULL;

  IF COL_LENGTH('dbo.steps','output_uri') IS NULL
    ALTER TABLE dbo.steps ADD output_uri NVARCHAR(1024) NULL;

  IF COL_LENGTH('dbo.steps','params_json') IS NULL
    ALTER TABLE dbo.steps ADD params_json NVARCHAR(MAX) NULL;

  IF COL_LENGTH('dbo.steps','ruleset_version') IS NULL
    ALTER TABLE dbo.steps ADD ruleset_version NVARCHAR(64) NULL;

  IF COL_LENGTH('dbo.steps','started_at') IS NULL
    ALTER TABLE dbo.steps ADD started_at DATETIME2 NULL;

  IF COL_LENGTH('dbo.steps','ended_at') IS NULL
    ALTER TABLE dbo.steps ADD ended_at DATETIME2 NULL;

  IF COL_LENGTH('dbo.steps','output_hash') IS NULL
    ALTER TABLE dbo.steps ADD output_hash NVARCHAR(128) NULL;

  IF COL_LENGTH('dbo.steps','error') IS NULL
    ALTER TABLE dbo.steps ADD error NVARCHAR(1024) NULL;
END
GO

-- ===== audit_log: ensure columns exist =====
IF OBJECT_ID('dbo.audit_log','U') IS NOT NULL
BEGIN
  IF COL_LENGTH('dbo.audit_log','actor') IS NULL
    ALTER TABLE dbo.audit_log ADD actor NVARCHAR(64) NULL;

  IF COL_LENGTH('dbo.audit_log','action') IS NULL
    ALTER TABLE dbo.audit_log ADD action NVARCHAR(64) NULL;

  IF COL_LENGTH('dbo.audit_log','job_id') IS NULL
    ALTER TABLE dbo.audit_log ADD job_id NVARCHAR(64) NULL;

  IF COL_LENGTH('dbo.audit_log','step_id') IS NULL
    ALTER TABLE dbo.audit_log ADD step_id NVARCHAR(64) NULL;

  IF COL_LENGTH('dbo.audit_log','detail_json') IS NULL
    ALTER TABLE dbo.audit_log ADD detail_json NVARCHAR(MAX) NULL;

  IF COL_LENGTH('dbo.audit_log','created_at') IS NULL
    ALTER TABLE dbo.audit_log ADD created_at DATETIME2 NOT NULL CONSTRAINT DF_audit_log_created_at DEFAULT SYSUTCDATETIME();
END
GO
