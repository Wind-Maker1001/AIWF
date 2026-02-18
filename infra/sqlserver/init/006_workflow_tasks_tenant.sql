USE [AIWF];
GO

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NOT NULL
BEGIN
  IF COL_LENGTH('dbo.workflow_tasks','tenant_id') IS NULL
  BEGIN
    ALTER TABLE dbo.workflow_tasks ADD tenant_id NVARCHAR(64) NOT NULL CONSTRAINT DF_workflow_tasks_tenant DEFAULT N'default';
  END
END
GO

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NOT NULL
BEGIN
  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_workflow_tasks_tenant_status' AND object_id=OBJECT_ID('dbo.workflow_tasks'))
    CREATE INDEX IX_workflow_tasks_tenant_status ON dbo.workflow_tasks(tenant_id, status);
END
GO
