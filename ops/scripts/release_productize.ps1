param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$Channel = "stable",
  [switch]$IncludeBundledTools,
  [switch]$CollectBundledTools,
  [string]$EnvFile = "",
  [switch]$SkipSqlConnectivityGate,
  [switch]$SkipRoutingBenchGate,
  [switch]$SkipRustTransformBenchGate,
  [switch]$SkipOpenApiSdkSyncGate,
  [int]$RustBenchRows = 120000,
  [int]$RustBenchRuns = 4,
  [int]$RustBenchWarmup = 1,
  [double]$RustBenchMinSpeedup = 1.03,
  [double]$RustBenchMinArrowSpeedup = 0.95,
  [switch]$RustBenchUpdateProfileOnPass
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$pkg = Join-Path $PSScriptRoot "package_offline_bundle.ps1"
$zip = Join-Path $PSScriptRoot "zip_offline_bundle.ps1"
$asyncTrend = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$rustTransformBenchGate = Join-Path $PSScriptRoot "check_rust_transform_bench_gate.ps1"
$sqlConnectivityGate = Join-Path $PSScriptRoot "check_sql_connectivity.ps1"
$openApiSdkSyncGate = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$openApiSdkSyncGateStatus = "skipped"
$openApiSdkSyncGateCheckedAt = ""
$openApiSdkSyncGateError = ""
$outRoot = Join-Path $root "release"
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}

if (-not $SkipOpenApiSdkSyncGate) {
  if (-not (Test-Path $openApiSdkSyncGate)) { throw "openapi/sdk sync gate script missing: $openApiSdkSyncGate" }
  Info "running openapi/sdk sync release gate"
  powershell -ExecutionPolicy Bypass -File $openApiSdkSyncGate
  $openApiSdkSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $openApiSdkSyncGateStatus = "failed"
    $openApiSdkSyncGateError = "check_openapi_sdk_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by openapi/sdk sync gate"
  }
  $openApiSdkSyncGateStatus = "passed"
  Ok "openapi/sdk sync release gate passed"
} else {
  $openApiSdkSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip openapi/sdk sync release gate" -ForegroundColor Yellow
}

if (-not $SkipSqlConnectivityGate) {
  if (-not (Test-Path $sqlConnectivityGate)) { throw "sql connectivity gate script missing: $sqlConnectivityGate" }
  Info "running SQL connectivity release gate"
  powershell -ExecutionPolicy Bypass -File $sqlConnectivityGate -EnvFile $EnvFile -SkipWhenTaskStoreNotSql
  if ($LASTEXITCODE -ne 0) { throw "release blocked by SQL connectivity gate" }
  Ok "SQL connectivity release gate passed"
} else {
  Write-Host "[WARN] skip SQL connectivity release gate" -ForegroundColor Yellow
}

if (-not $SkipRoutingBenchGate) {
  $desktopDir = Join-Path $root "apps\dify-desktop"
  if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }
  Info "running workflow routing release gate"
  Push-Location $desktopDir
  try {
    $prevMaxPerEdge = $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE
    $prevMaxWorstPerEdge = $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE
    $prevMaxFallbackRatio = $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO
    $prevMaxFixedFallbackRatio = $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO
    $prevTrendMedian = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX
    $prevTrendWorst = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX
    try {
      $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE = "125"
      $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE = "165"
      $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO = "0.48"
      $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO = "0.48"
      $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX = "115"
      $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX = "155"
      npm run bench:routing
      if ($LASTEXITCODE -ne 0) { throw "release blocked by routing benchmark gate" }
    }
    finally {
      if ($null -eq $prevMaxPerEdge) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE = $prevMaxPerEdge }
      if ($null -eq $prevMaxWorstPerEdge) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE = $prevMaxWorstPerEdge }
      if ($null -eq $prevMaxFallbackRatio) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO = $prevMaxFallbackRatio }
      if ($null -eq $prevMaxFixedFallbackRatio) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO = $prevMaxFixedFallbackRatio }
      if ($null -eq $prevTrendMedian) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX = $prevTrendMedian }
      if ($null -eq $prevTrendWorst) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX = $prevTrendWorst }
    }
  }
  finally {
    Pop-Location
  }
  Ok "workflow routing release gate passed"
} else {
  Write-Host "[WARN] skip workflow routing release gate" -ForegroundColor Yellow
}

if (Test-Path $asyncTrend) {
  Info "running async bench trend release gate"
  powershell -ExecutionPolicy Bypass -File $asyncTrend -AccelUrl "http://127.0.0.1:18082" -Tasks 10 -RowsPerTask 500 -TimeoutSeconds 90
  if ($LASTEXITCODE -ne 0) { throw "release blocked by async bench trend gate" }
  Ok "async bench trend release gate passed"
}

if (-not $SkipRustTransformBenchGate) {
  if (-not (Test-Path $rustTransformBenchGate)) { throw "rust transform benchmark gate script missing: $rustTransformBenchGate" }
  Info "running rust transform benchmark release gate"
  $rustBenchArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $rustTransformBenchGate,
    "-AccelUrl", "http://127.0.0.1:18082",
    "-Rows", "$RustBenchRows",
    "-Runs", "$RustBenchRuns",
    "-Warmup", "$RustBenchWarmup",
    "-MinSpeedup", "$RustBenchMinSpeedup",
    "-MinArrowSpeedup", "$RustBenchMinArrowSpeedup",
    "-EnforceArrowAlways"
  )
  if ($RustBenchUpdateProfileOnPass) { $rustBenchArgs += "-UpdateProfileOnPass" }
  powershell @rustBenchArgs
  if ($LASTEXITCODE -ne 0) { throw "release blocked by rust transform benchmark gate" }
  Ok "rust transform benchmark release gate passed"
} else {
  Write-Host "[WARN] skip rust transform benchmark release gate" -ForegroundColor Yellow
}

foreach ($type in @("installer", "portable")) {
  Info "packaging $type"
  $args = @{
    Version = $Version
    PackageType = $type
    CleanOldReleases = $false
    ReleaseChannel = $Channel
    RequireChineseOcr = $true
    SkipOpenApiSdkSyncGate = $true
  }
  if ($IncludeBundledTools) { $args.IncludeBundledTools = $true }
  if ($CollectBundledTools) { $args.CollectBundledTools = $true }
  & $pkg @args

  Info "zipping $type"
  & $zip -Version $Version -PackageType $type

  $bundleDir = Join-Path $root ("release\offline_bundle_{0}_{1}\AIWF_Offline_Bundle" -f $Version, $type)
  $zipPath = Join-Path $root ("release\offline_bundle_{0}_{1}\AIWF_Offline_Bundle.zip" -f $Version, $type)
  if (-not (Test-Path $bundleDir)) { throw "bundle missing: $bundleDir" }
  if (-not (Test-Path $zipPath)) { throw "zip missing: $zipPath" }

  $sumPath = Join-Path $bundleDir "SHA256SUMS.txt"
  if (-not (Test-Path $sumPath)) { throw "sha sums missing: $sumPath" }

  $manifest = Join-Path $bundleDir "manifest.json"
  $notes = Join-Path $bundleDir "RELEASE_NOTES.md"
  if (-not (Test-Path $manifest)) { throw "manifest missing: $manifest" }
  if (-not (Test-Path $notes)) { throw "release notes missing: $notes" }
}

$auditPath = Join-Path $outRoot ("release_gate_audit_{0}.json" -f $Version)
$audit = [ordered]@{
  version = $Version
  channel = $Channel
  generated_at = (Get-Date).ToString("s")
  gates = [ordered]@{
    openapi_sdk_sync = [ordered]@{
      status = $openApiSdkSyncGateStatus
      checked_at = $openApiSdkSyncGateCheckedAt
      script = "ops/scripts/check_openapi_sdk_sync.ps1"
      error = $openApiSdkSyncGateError
    }
  }
}
($audit | ConvertTo-Json -Depth 6) | Set-Content $auditPath -Encoding UTF8
Ok "release gate audit written: $auditPath"

Ok "release ready: release/offline_bundle_${Version}_installer and _portable"
