USE [AIWF];
GO

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NOT NULL
BEGIN
  IF COL_LENGTH('dbo.workflow_tasks','idempotency_key') IS NULL
    ALTER TABLE dbo.workflow_tasks ADD idempotency_key NVARCHAR(128) NULL;

  IF COL_LENGTH('dbo.workflow_tasks','attempts') IS NULL
    ALTER TABLE dbo.workflow_tasks ADD attempts INT NOT NULL CONSTRAINT DF_workflow_tasks_attempts DEFAULT 0;
END
GO
