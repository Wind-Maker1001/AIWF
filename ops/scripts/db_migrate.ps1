param(
  [string]$SqlHost = "127.0.0.1",
  [int]$SqlPort = 1433,
  [string]$DbName = "AIWF",
  [string]$SqlUser = "sa",
  [Parameter(Mandatory=$true)]
  [string]$SqlPassword,
  [string]$Root = "",
  [string]$AppUser,
  [string]$AppPassword
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

function Run-SqlFile([string]$Server, [string]$User, [string]$Pass, [string]$FilePath) {
  & sqlcmd -S $Server -U $User -P $Pass -b -i $FilePath
  if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed running file: $FilePath" }
}

function Run-Sql([string]$Server, [string]$User, [string]$Pass, [string]$Query) {
  & sqlcmd -S $Server -U $User -P $Pass -b -Q $Query
  if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed running query" }
}

if (-not (Get-Command sqlcmd -ErrorAction SilentlyContinue)) {
  throw "sqlcmd not found in PATH"
}

$server = "$SqlHost,$SqlPort"
$initDir = Join-Path $Root "infra\sqlserver\init"

$files = @(
  Join-Path $initDir "001_init.sql",
  Join-Path $initDir "002_control_plane_extend.sql",
  Join-Path $initDir "004_fix_steps_audit.sql",
  Join-Path $initDir "005_workflow_tasks.sql",
  Join-Path $initDir "006_workflow_tasks_tenant.sql"
)

Info "Migrating SQL Server $server / DB $DbName"
foreach($f in $files){
  if(-not (Test-Path $f)){ throw "missing migration file: $f" }
  Info "Running $f"
  Run-SqlFile $server $SqlUser $SqlPassword $f
}

if ($AppUser -and $AppPassword) {
  Info "Creating/updating app login+user: $AppUser"

  $q1 = @"
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'$AppUser')
BEGIN
  CREATE LOGIN [$AppUser] WITH PASSWORD = N'$AppPassword', CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
END
"@
  Run-Sql $server $SqlUser $SqlPassword $q1

  $q2 = @"
USE [$DbName];
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$AppUser')
BEGIN
  CREATE USER [$AppUser] FOR LOGIN [$AppUser];
END
ALTER ROLE db_datareader ADD MEMBER [$AppUser];
ALTER ROLE db_datawriter ADD MEMBER [$AppUser];
GRANT EXECUTE TO [$AppUser];
"@
  Run-Sql $server $SqlUser $SqlPassword $q2
}

Ok "DB migration done. Canonical chain: 001 -> 002 -> 004 -> 005 -> 006"
