param(
  [string]$SqlHost = "127.0.0.1",
  [int]$SqlPort = 1433,
  [string]$DbName = "AIWF",
  [string]$SqlUser = "sa",
  [string]$SqlPassword,
  [switch]$UseTrustedAuth,
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

function Run-SqlFile([string]$Server, [string]$User, [string]$Pass, [bool]$Trusted, [string]$FilePath) {
  if ($Trusted) {
    & sqlcmd -S $Server -E -b -i $FilePath
  } else {
    & sqlcmd -S $Server -U $User -P $Pass -b -i $FilePath
  }
  if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed running file: $FilePath" }
}

function Run-Sql([string]$Server, [string]$User, [string]$Pass, [bool]$Trusted, [string]$Query) {
  if ($Trusted) {
    & sqlcmd -S $Server -E -b -Q $Query
  } else {
    & sqlcmd -S $Server -U $User -P $Pass -b -Q $Query
  }
  if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed running query" }
}

if (-not (Get-Command sqlcmd -ErrorAction SilentlyContinue)) {
  throw "sqlcmd not found in PATH"
}

$server = "$SqlHost,$SqlPort"
$initDir = Join-Path $Root "infra\sqlserver\init"

if (-not $UseTrustedAuth -and [string]::IsNullOrWhiteSpace($SqlPassword)) {
  throw "SqlPassword is required unless -UseTrustedAuth is specified"
}

$files = Get-ChildItem -Path $initDir -File -Filter "*.sql" |
  Sort-Object Name |
  Select-Object -ExpandProperty FullName

Info "Migrating SQL Server $server / DB $DbName"
foreach($f in $files){
  if(-not (Test-Path $f)){ throw "missing migration file: $f" }
  Info "Running $f"
  Run-SqlFile $server $SqlUser $SqlPassword $UseTrustedAuth $f
}

if ($AppUser -and $AppPassword) {
  Info "Creating/updating app login+user: $AppUser"

  $q1 = @"
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'$AppUser')
BEGIN
  CREATE LOGIN [$AppUser] WITH PASSWORD = N'$AppPassword', CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;
END
"@
  Run-Sql $server $SqlUser $SqlPassword $UseTrustedAuth $q1

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
  Run-Sql $server $SqlUser $SqlPassword $UseTrustedAuth $q2
}

Ok ("DB migration done. Applied files: {0}" -f (($files | ForEach-Object { Split-Path $_ -Leaf }) -join ", "))
