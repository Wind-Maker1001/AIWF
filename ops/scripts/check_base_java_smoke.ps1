param(
  [ValidateSet("Auto","Live","Contract")]
  [string]$Mode = "Auto",
  [string]$EnvFile = "",
  [string]$BaseUrl = "",
  [string]$ProjectDir = ""
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

    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }

    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

function WithMavenJvmFlags([scriptblock]$Action) {
  $prev = $env:MAVEN_OPTS
  $flag = "-XX:+EnableDynamicAgentLoading"
  if ([string]::IsNullOrWhiteSpace($prev)) {
    $env:MAVEN_OPTS = $flag
  } elseif ($prev -notmatch [regex]::Escape($flag)) {
    $env:MAVEN_OPTS = "$prev $flag"
  }
  try {
    & $Action
  }
  finally {
    if ($null -eq $prev) { Remove-Item Env:MAVEN_OPTS -ErrorAction SilentlyContinue }
    else { $env:MAVEN_OPTS = $prev }
  }
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

function Invoke-JsonAllowHttpError {
  param(
    [string]$Method = "GET",
    [string]$Uri,
    [string]$ContentType = "application/json",
    [string]$Body = "",
    [int]$TimeoutSec = 15
  )

  try {
    if ($Method -eq "GET") {
      $res = Invoke-RestMethod -Uri $Uri -Method Get -TimeoutSec $TimeoutSec
    } else {
      $res = Invoke-RestMethod -Uri $Uri -Method $Method -ContentType $ContentType -Body $Body -TimeoutSec $TimeoutSec
    }
    return @{ ok = $true; status_code = 200; body = $res }
  } catch {
    $resp = $_.Exception.Response
    if ($null -eq $resp) { throw }

    $status = 0
    try { $status = [int]$resp.StatusCode } catch {}
    $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
    $raw = $reader.ReadToEnd()
    $obj = $null
    try { $obj = $raw | ConvertFrom-Json } catch { $obj = @{ raw = $raw } }
    return @{ ok = $false; status_code = $status; body = $obj }
  }
}

function Assert-HttpStatus($Result, [int]$ExpectedStatus, [string]$Label) {
  if ($Result.status_code -ne $ExpectedStatus) {
    throw "$Label expected HTTP $ExpectedStatus, got $($Result.status_code)"
  }
}

function Run-LiveSmoke([string]$ResolvedBaseUrl) {
  Info "running live base-java smoke against $ResolvedBaseUrl"

  Invoke-WithRetry -Label "base liveness" -Action {
    $live = Invoke-JsonAllowHttpError -Uri "$ResolvedBaseUrl/actuator/health/liveness"
    Assert-HttpStatus $live 200 "liveness"
    $live
  } | Out-Null
  Ok "base liveness passed"

  $dify = Invoke-WithRetry -Label "dify health" -Action {
    $result = Invoke-JsonAllowHttpError -Uri "$ResolvedBaseUrl/api/v1/integrations/dify/health"
    Assert-HttpStatus $result 200 "dify health"
    if (-not $result.body.ok) { throw "dify health returned ok=false" }
    $result
  }
  Ok "dify integration health passed"

  $glue = Invoke-WithRetry -Label "glue health proxy" -Action {
    $result = Invoke-JsonAllowHttpError -Uri "$ResolvedBaseUrl/api/v1/jobs/glue/health"
    Assert-HttpStatus $result 200 "glue health proxy"
    if (-not ($result.body.PSObject.Properties.Name -contains "ok")) {
      throw "glue health proxy missing ok field"
    }
    $result
  }
  Ok "glue health proxy shape passed"

  $missingTask = Invoke-JsonAllowHttpError -Method Post -Uri "$ResolvedBaseUrl/api/v1/runtime/tasks/upsert" -Body '{"status":"queued"}'
  Assert-HttpStatus $missingTask 400 "runtime upsert validation"
  if ($missingTask.body.error -ne "task_id_required") {
    throw "runtime upsert validation returned unexpected error: $($missingTask.body.error)"
  }
  Ok "runtime task validation passed"

  $badJson = Invoke-JsonAllowHttpError -Method Post -Uri "$ResolvedBaseUrl/api/v1/jobs/create?owner=local" -Body '{'
  Assert-HttpStatus $badJson 400 "create job invalid json"
  if (-not ($badJson.body.PSObject.Properties.Name -contains "error")) {
    throw "create job invalid json missing error field"
  }
  Ok "invalid json handling passed"

  Write-Host ""
  Write-Host "=== Base Java Live Smoke Result ==="
  Write-Host "base_url          : $ResolvedBaseUrl"
  Write-Host "dify_health_ok    : $($dify.body.ok)"
  Write-Host "glue_health_proxy : $($glue.body.ok)"
  Write-Host "task_validation   : $($missingTask.body.error)"
  Write-Host "bad_json_error    : $($badJson.body.error)"
}

function Run-ContractSmoke([string]$ResolvedProjectDir) {
  if (-not (Get-Command mvn -ErrorAction SilentlyContinue)) {
    throw "mvn not found in PATH"
  }
  if (-not (Test-Path $ResolvedProjectDir)) {
    throw "base-java dir not found: $ResolvedProjectDir"
  }

  $tests = @(
    "GlueClientTest",
    "ApiKeyFilterTest",
    "JobRepositoryTest",
    "RuntimeTaskRepositoryTest",
    "JobServiceTest",
    "JobStatusServiceTest",
    "JobControllerContractTest",
    "CallbackControllerTest",
    "RuntimeTaskControllerTest",
    "ToolsControllerTest",
    "DifyControllerTest"
  )
  $testSelector = ($tests -join ",")

  Info "running contract smoke via Maven: $testSelector"
  Push-Location $ResolvedProjectDir
  try {
    WithMavenJvmFlags { mvn -q "-Dtest=$testSelector" test }
  }
  finally {
    Pop-Location
  }

  Write-Host ""
  Write-Host "=== Base Java Contract Smoke Result ==="
  Write-Host "project_dir : $ResolvedProjectDir"
  Write-Host "tests       : $testSelector"
  Ok "base-java contract smoke passed"
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\base-java"
}

Import-DotEnv $EnvFile

if (-not $BaseUrl) {
  $BaseUrl = if ($env:AIWF_BASE_URL) { $env:AIWF_BASE_URL } else { "http://127.0.0.1:18080" }
}

if ($Mode -eq "Live") {
  Run-LiveSmoke -ResolvedBaseUrl $BaseUrl
  exit 0
}

if ($Mode -eq "Contract") {
  Run-ContractSmoke -ResolvedProjectDir $ProjectDir
  exit 0
}

try {
  Run-LiveSmoke -ResolvedBaseUrl $BaseUrl
} catch {
  Warn "live smoke unavailable or failed, fallback to contract smoke: $($_.Exception.Message)"
  Run-ContractSmoke -ResolvedProjectDir $ProjectDir
}
