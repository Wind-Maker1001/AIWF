USE [AIWF];
GO

IF OBJECT_ID('dbo.workflow_tasks', 'U') IS NOT NULL
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'ux_workflow_tasks_tenant_idempotency'
      AND object_id = OBJECT_ID(N'dbo.workflow_tasks')
  )
  BEGIN
    CREATE UNIQUE INDEX ux_workflow_tasks_tenant_idempotency
      ON dbo.workflow_tasks (tenant_id, operator, idempotency_key)
      WHERE idempotency_key IS NOT NULL;
  END
END
GO
