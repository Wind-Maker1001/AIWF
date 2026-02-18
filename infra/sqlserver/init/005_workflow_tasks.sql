USE [AIWF];
GO

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.workflow_tasks (
    task_id           NVARCHAR(64)   NOT NULL PRIMARY KEY,
    operator          NVARCHAR(64)   NOT NULL,
    status            NVARCHAR(32)   NOT NULL,
    created_at_epoch  BIGINT         NOT NULL,
    updated_at_epoch  BIGINT         NOT NULL,
    result_json       NVARCHAR(MAX)  NULL,
    error             NVARCHAR(MAX)  NULL,
    source            NVARCHAR(32)   NOT NULL DEFAULT N'accel-rust'
  );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_workflow_tasks_status' AND object_id=OBJECT_ID('dbo.workflow_tasks'))
  CREATE INDEX IX_workflow_tasks_status ON dbo.workflow_tasks(status);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_workflow_tasks_updated' AND object_id=OBJECT_ID('dbo.workflow_tasks'))
  CREATE INDEX IX_workflow_tasks_updated ON dbo.workflow_tasks(updated_at_epoch DESC);
GO
