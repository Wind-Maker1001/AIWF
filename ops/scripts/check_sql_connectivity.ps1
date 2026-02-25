param(
  [string]$EnvFile = "",
  [switch]$SkipWhenTaskStoreNotSql
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf("=")
    if ($idx -le 0) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

function Resolve-SqlCmdPath {
  $cmd = Get-Command sqlcmd -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  $fallbacks = @(
    "C:\Program Files\SqlCmd\sqlcmd.exe",
    "D:\SQL Server\Shared Function\Client SDK\ODBC\170\Tools\Binn\SQLCMD.EXE"
  )
  foreach ($p in $fallbacks) {
    if (Test-Path $p) { return $p }
  }
  return $null
}

function Invoke-SqlScalar {
  param(
    [string]$SqlCmdPath,
    [string]$Server,
    [string]$DbName,
    [string]$User,
    [string]$Password,
    [bool]$UseTrustedAuth = $false,
    [string]$Query
  )
  if ($UseTrustedAuth) {
    $lines = & $SqlCmdPath -S $Server -E -d $DbName -h -1 -W -Q $Query 2>&1
  } else {
    $lines = & $SqlCmdPath -S $Server -U $User -P $Password -d $DbName -h -1 -W -Q $Query 2>&1
  }
  if ($LASTEXITCODE -ne 0) {
    $msg = ($lines | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ -ne "" } | Select-Object -Last 1)
    if (-not $msg) { $msg = "unknown sqlcmd error" }
    throw "sqlcmd query failed: $msg"
  }
  $clean = @($lines | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ -ne "" })
  if ($clean.Count -lt 1) {
    throw "sqlcmd query returned no rows"
  }
  return [int]$clean[-1]
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
Import-DotEnv $EnvFile

$taskStoreRemote = [string]($env:AIWF_RUST_TASK_STORE_REMOTE)
$taskStoreBackend = [string]($env:AIWF_RUST_TASK_STORE_BACKEND)
$taskStoreIsSql = [string]::Equals($taskStoreRemote, "true", [System.StringComparison]::OrdinalIgnoreCase) -and @("sqlcmd", "odbc") -contains $taskStoreBackend
if ($SkipWhenTaskStoreNotSql -and -not $taskStoreIsSql) {
  Warn "skip SQL connectivity gate: task store backend is not sqlcmd/odbc remote mode"
  exit 0
}

$sqlCmdPath = Resolve-SqlCmdPath
if (-not $sqlCmdPath) {
  throw "sqlcmd not found in PATH"
}

$sqlHost = if ($env:AIWF_SQL_HOST) { $env:AIWF_SQL_HOST } else { "127.0.0.1" }
$sqlPort = if ($env:AIWF_SQL_PORT) { $env:AIWF_SQL_PORT } else { "1433" }
$sqlDb = if ($env:AIWF_SQL_DB) { $env:AIWF_SQL_DB } else { "AIWF" }
$sqlUser = if ($env:AIWF_SQL_USER) { $env:AIWF_SQL_USER } else { "aiwf_app" }
$sqlPassword = if ($env:AIWF_SQL_PASSWORD) { $env:AIWF_SQL_PASSWORD } else { "" }
$trustedPref = if ($env:AIWF_SQL_TRUSTED) { $env:AIWF_SQL_TRUSTED } else { "" }

$useTrustedAuth = $false
if ([string]::IsNullOrWhiteSpace($trustedPref) -eq $false) {
  $useTrustedAuth = [string]::Equals($trustedPref, "1", [System.StringComparison]::OrdinalIgnoreCase) `
    -or [string]::Equals($trustedPref, "true", [System.StringComparison]::OrdinalIgnoreCase) `
    -or [string]::Equals($trustedPref, "yes", [System.StringComparison]::OrdinalIgnoreCase)
} elseif (-not $sqlPassword -or $sqlPassword -match "^__SET_.*__$") {
  $useTrustedAuth = $true
}

if (-not $useTrustedAuth) {
  if (-not $sqlPassword) {
    throw "AIWF_SQL_PASSWORD is empty; set it or enable AIWF_SQL_TRUSTED=1"
  }
  if ($sqlPassword -match "^__SET_.*__$") {
    throw "AIWF_SQL_PASSWORD is still a placeholder; set real password or enable AIWF_SQL_TRUSTED=1"
  }
}

$server = "$sqlHost,$sqlPort"
Info ("checking SQL connectivity: server={0} db={1} auth={2}" -f $server, $sqlDb, ($(if($useTrustedAuth){"trusted"}else{"sql_user"})))

$dbOk = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -UseTrustedAuth $useTrustedAuth -Query "SET NOCOUNT ON; SELECT 1;"
if ($dbOk -ne 1) { throw "SQL connectivity gate failed: SELECT 1 unexpected result $dbOk" }

$jobsExists = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -UseTrustedAuth $useTrustedAuth -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.jobs') AND type IN (N'U');"
$stepsExists = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -UseTrustedAuth $useTrustedAuth -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.steps') AND type IN (N'U');"
$artsExists = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -UseTrustedAuth $useTrustedAuth -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.artifacts') AND type IN (N'U');"

if ($jobsExists -lt 1 -or $stepsExists -lt 1 -or $artsExists -lt 1) {
  throw "SQL schema gate failed: required tables dbo.jobs/dbo.steps/dbo.artifacts are missing"
}

Ok "SQL connectivity gate passed"
