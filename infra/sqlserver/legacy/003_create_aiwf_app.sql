-- LEGACY NOTICE: This script is kept for historical compatibility.
-- Canonical entrypoint is ops/scripts/db_migrate.ps1.
-- Security note: do not store real passwords in this file.
-- If you really need to run this script directly, replace <SET_STRONG_PASSWORD>.
-- Create login (server-level)
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'aiwf_app')
BEGIN
  CREATE LOGIN [aiwf_app]
    WITH PASSWORD = N'<SET_STRONG_PASSWORD>',
         CHECK_POLICY = ON,
         CHECK_EXPIRATION = OFF;
END
GO

USE [AIWF];
GO

-- Create user (db-level)
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'aiwf_app')
BEGIN
  CREATE USER [aiwf_app] FOR LOGIN [aiwf_app];
END
GO

-- Minimal practical permissions for control-plane CRUD
ALTER ROLE db_datareader ADD MEMBER [aiwf_app];
ALTER ROLE db_datawriter ADD MEMBER [aiwf_app];
GRANT EXECUTE TO [aiwf_app];
GO
