param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [switch]$SkipSqlVerify,
  [switch]$SkipOfficeQualityGate,
  [switch]$WithInvalidParquetFallbackTest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $EnvFile) {
  $root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
  $EnvFile = Join-Path $root "ops\config\dev.env"
}

function Invoke-WithRetry {
  param(
    [scriptblock]$Action,
    [int]$MaxAttempts = 4,
    [int]$DelaySeconds = 2,
    [string]$Label = "request"
  )

  for ($i = 1; $i -le $MaxAttempts; $i++) {
    try {
      return & $Action
    } catch {
      if ($i -ge $MaxAttempts) { throw }
      Warn "$Label failed on attempt $i/$MaxAttempts, retrying in ${DelaySeconds}s: $($_.Exception.Message)"
      Start-Sleep -Seconds $DelaySeconds
    }
  }
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }

  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }

    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }

    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

function Resolve-SqlCmdPath {
  $cmd = Get-Command sqlcmd -ErrorAction SilentlyContinue
  if ($cmd) {
    return $cmd.Source
  }
  $fallbacks = @(
    "C:\Program Files\SqlCmd\sqlcmd.exe",
    "D:\SQL Server\Shared Function\Client SDK\ODBC\170\Tools\Binn\SQLCMD.EXE"
  )
  foreach($p in $fallbacks){
    if(Test-Path $p){ return $p }
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
    [string]$Query
  )
  $lines = & $SqlCmdPath -S $Server -U $User -P $Password -d $DbName -h -1 -W -Q $Query
  if ($LASTEXITCODE -ne 0) {
    throw "sqlcmd query failed"
  }
  $clean = @($lines | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ -ne "" })
  if ($clean.Count -lt 1) {
    throw "sqlcmd query returned no rows"
  }
  return [int]$clean[-1]
}

Import-DotEnv $EnvFile

$base = if ($env:AIWF_BASE_URL) { $env:AIWF_BASE_URL } else { "http://127.0.0.1:18080" }
$glue = if ($env:AIWF_GLUE_URL) { $env:AIWF_GLUE_URL } else { "http://127.0.0.1:18081" }

Info "base=$base"
Info "glue=$glue"

$h1 = Invoke-WithRetry -Label "base health" -Action { Invoke-RestMethod "$base/actuator/health" -Method Get }
$h2 = Invoke-WithRetry -Label "glue health" -Action { Invoke-RestMethod "$glue/health" -Method Get }
$h3 = Invoke-WithRetry -Label "base->glue health" -Action { Invoke-RestMethod "$base/api/v1/jobs/glue/health" -Method Get }
Ok "health checks passed"

$job = Invoke-WithRetry -Label "create job" -Action {
  Invoke-RestMethod "$base/api/v1/tools/create_job?owner=$Owner" -Method Post -ContentType "application/json" -Body "{}"
}
$jobId = $job.job_id
Info "created job_id=$jobId"

$runBody = @{ actor = "local"; ruleset_version = "v1"; params = @{} } | ConvertTo-Json -Depth 5
$run = Invoke-WithRetry -Label "run cleaning flow" -Action {
  Invoke-RestMethod "$base/api/v1/jobs/$jobId/run/cleaning" -Method Post -ContentType "application/json" -Body $runBody
}

$steps = Invoke-WithRetry -Label "list steps" -Action { Invoke-RestMethod "$base/api/v1/jobs/$jobId/steps" -Method Get }
$arts = Invoke-WithRetry -Label "list artifacts" -Action { Invoke-RestMethod "$base/api/v1/jobs/$jobId/artifacts" -Method Get }

Write-Host ""
Write-Host "=== Smoke Result ==="
Write-Host "job_id      : $jobId"
Write-Host "run_ok      : $($run.ok)"
Write-Host "run_seconds : $($run.seconds)"
Write-Host "steps_count : $($steps.Count)"
Write-Host "artifacts   : $($arts.Count)"

if (-not $SkipSqlVerify) {
  $sqlCmdPath = Resolve-SqlCmdPath
  if (-not $sqlCmdPath) {
    throw "sqlcmd not found; cannot verify SQL persistence (use -SkipSqlVerify to bypass)"
  }
  $sqlHost = if ($env:AIWF_SQL_HOST) { $env:AIWF_SQL_HOST } else { "127.0.0.1" }
  $sqlPort = if ($env:AIWF_SQL_PORT) { $env:AIWF_SQL_PORT } else { "1433" }
  $sqlDb = if ($env:AIWF_SQL_DB) { $env:AIWF_SQL_DB } else { "AIWF" }
  $sqlUser = if ($env:AIWF_SQL_USER) { $env:AIWF_SQL_USER } else { "aiwf_app" }
  $sqlPassword = if ($env:AIWF_SQL_PASSWORD) { $env:AIWF_SQL_PASSWORD } else { "" }
  if (-not $sqlPassword) {
    throw "AIWF_SQL_PASSWORD is empty; cannot verify SQL persistence"
  }
  $safeJobId = $jobId.Replace("'", "''")
  $server = "$sqlHost,$sqlPort"
  $jobCount = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM dbo.jobs WHERE job_id = N'$safeJobId';"
  $stepCount = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM dbo.steps WHERE job_id = N'$safeJobId';"
  $artifactCount = Invoke-SqlScalar -SqlCmdPath $sqlCmdPath -Server $server -DbName $sqlDb -User $sqlUser -Password $sqlPassword -Query "SET NOCOUNT ON; SELECT COUNT(1) FROM dbo.artifacts WHERE job_id = N'$safeJobId';"
  Write-Host "sql_jobs    : $jobCount"
  Write-Host "sql_steps   : $stepCount"
  Write-Host "sql_arts    : $artifactCount"
  if ($jobCount -lt 1 -or $stepCount -lt 1 -or $artifactCount -lt 1) {
    throw "SQL persistence verification failed"
  }
  Ok "SQL persistence verified"
}

if (-not $SkipOfficeQualityGate) {
  $qualityScript = Join-Path $PSScriptRoot "check_office_artifacts_quality.ps1"
  if (-not (Test-Path $qualityScript)) {
    throw "office quality script not found: $qualityScript"
  }
  $xlsx = ($arts | Where-Object { $_.kind -eq "xlsx" } | Select-Object -First 1)
  $docx = ($arts | Where-Object { $_.kind -eq "docx" } | Select-Object -First 1)
  $pptx = ($arts | Where-Object { $_.kind -eq "pptx" } | Select-Object -First 1)
  if (-not $xlsx -or -not $docx -or -not $pptx) {
    throw "missing office artifacts for quality gate"
  }
  powershell -ExecutionPolicy Bypass -File $qualityScript -XlsxPath $xlsx.path -DocxPath $docx.path -PptxPath $pptx.path
  if ($LASTEXITCODE -ne 0) {
    throw "office artifact quality gate failed"
  }
  Ok "office artifact quality verified"
}

Ok "smoke test finished"

if ($WithInvalidParquetFallbackTest) {
  $fallbackScript = Join-Path $PSScriptRoot "test_invalid_parquet_fallback.ps1"
  if (-not (Test-Path $fallbackScript)) {
    Warn "invalid parquet fallback script not found: $fallbackScript"
    exit 1
  }

  Write-Host ""
  Info "running invalid parquet fallback integration test"
  powershell -ExecutionPolicy Bypass -File $fallbackScript -EnvFile $EnvFile -Owner $Owner
  if ($LASTEXITCODE -ne 0) {
    throw "invalid parquet fallback integration test failed"
  }
  Ok "fallback integration test finished"
}
