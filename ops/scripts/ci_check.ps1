param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [switch]$SkipToolChecks,
  [switch]$SkipDocsChecks,
  [switch]$SkipEncodingChecks,
  [switch]$SkipJavaTests,
  [switch]$SkipRustTests,
  [switch]$SkipPythonTests,
  [switch]$SkipRegressionQuality,
  [switch]$SkipDesktopUiTests,
  [switch]$SkipDesktopStress,
  [switch]$SkipDesktopPackageTests,
  [switch]$SkipRoutingBench,
  [switch]$SkipAsyncBench,
  [switch]$SkipOpenApiSdkSync,
  [switch]$SkipSecretScan,
  [switch]$SkipContractTests,
  [switch]$SkipChaosChecks,
  [switch]$SkipSmoke
  ,
  [switch]$SkipPostCleanup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
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
function WithNoDeprecation([scriptblock]$Action) {
  $prev = $env:NODE_OPTIONS
  if ([string]::IsNullOrWhiteSpace($prev)) {
    $env:NODE_OPTIONS = "--no-deprecation"
  } elseif ($prev -notmatch "(^| )--no-deprecation( |$)") {
    $env:NODE_OPTIONS = "$prev --no-deprecation"
  }
  try {
    & $Action
  }
  finally {
    if ($null -eq $prev) { Remove-Item Env:NODE_OPTIONS -ErrorAction SilentlyContinue }
    else { $env:NODE_OPTIONS = $prev }
  }
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
$rustDir = Join-Path $root "apps\accel-rust"
$pythonDir = Join-Path $root "apps\glue-python"
$desktopDir = Join-Path $root "apps\dify-desktop"
$javaDir = Join-Path $root "apps\base-java"
$smokeScript = Join-Path $PSScriptRoot "smoke_test.ps1"
$toolsScript = Join-Path $PSScriptRoot "check_dev_tools.ps1"
$runtimeDepsScript = Join-Path $PSScriptRoot "check_runtime_deps.ps1"
$docsCheckScript = Join-Path $PSScriptRoot "check_docs_links.ps1"
$encodingCheckScript = Join-Path $PSScriptRoot "check_encoding_health.ps1"
$desktopPkgCheckScript = Join-Path $PSScriptRoot "check_desktop_packaged_startup.ps1"
$desktopLitePkgCheckScript = Join-Path $PSScriptRoot "check_desktop_lite_packaged_startup.ps1"
$regressionQualityScript = Join-Path $PSScriptRoot "run_regression_quality.ps1"
$asyncBenchTrendScript = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$openApiSdkSyncScript = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$secretScanScript = Join-Path $PSScriptRoot "secret_scan.ps1"
$contractRustApiScript = Join-Path $PSScriptRoot "contract_test_rust_api.ps1"
$chaosTaskStoreScript = Join-Path $PSScriptRoot "chaos_task_store.ps1"
$cleanupScript = Join-Path $PSScriptRoot "clean_workspace_artifacts.ps1"

if (-not $SkipToolChecks) {
  if (Test-Path $toolsScript) {
    Info "running developer tool checks"
    powershell -ExecutionPolicy Bypass -File $toolsScript
    if ($LASTEXITCODE -ne 0) {
      throw "developer tool checks failed"
    }
    Ok "developer tool checks passed"
  }
  if (Test-Path $runtimeDepsScript) {
    Info "running runtime dependency checks"
    powershell -ExecutionPolicy Bypass -File $runtimeDepsScript
    if ($LASTEXITCODE -ne 0) {
      throw "runtime dependency checks failed"
    }
    Ok "runtime dependency checks passed"
  }
} else {
  Warn "skip tool checks"
}

if (-not $SkipDocsChecks) {
  if (Test-Path $docsCheckScript) {
    Info "running docs local link checks"
    powershell -ExecutionPolicy Bypass -File $docsCheckScript -IncludeReadme
    if ($LASTEXITCODE -ne 0) {
      throw "docs local link checks failed"
    }
    Ok "docs local link checks passed"
  }
} else {
  Warn "skip docs checks"
}

if (-not $SkipOpenApiSdkSync) {
  if (-not (Test-Path $openApiSdkSyncScript)) {
    throw "openapi sdk sync script not found: $openApiSdkSyncScript"
  }
  Info "running openapi/sdk sync checks"
  powershell -ExecutionPolicy Bypass -File $openApiSdkSyncScript
  if ($LASTEXITCODE -ne 0) {
    throw "openapi/sdk sync checks failed"
  }
  Ok "openapi/sdk sync checks passed"
} else {
  Warn "skip openapi/sdk sync checks"
}

if (-not $SkipSecretScan) {
  if (-not (Test-Path $secretScanScript)) {
    throw "secret scan script not found: $secretScanScript"
  }
  Info "running secret scan checks"
  powershell -ExecutionPolicy Bypass -File $secretScanScript -Root $root
  if ($LASTEXITCODE -ne 0) {
    throw "secret scan checks failed"
  }
  Ok "secret scan checks passed"
} else {
  Warn "skip secret scan checks"
}

if (-not $SkipEncodingChecks) {
  if (Test-Path $encodingCheckScript) {
    Info "running encoding health checks"
    powershell -ExecutionPolicy Bypass -File $encodingCheckScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      throw "encoding health checks failed"
    }
    Ok "encoding health checks passed"
  }
} else {
  Warn "skip encoding checks"
}

if (-not $SkipRustTests) {
  if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo not found in PATH"
  }
  if (-not (Test-Path $rustDir)) {
    throw "accel-rust dir not found: $rustDir"
  }

  Info "running accel-rust tests"
  Push-Location $rustDir
  try {
    cargo test -q
  }
  finally {
    Pop-Location
  }
  Ok "accel-rust tests passed"
} else {
  Warn "skip accel-rust tests"
}

if (-not $SkipJavaTests) {
  if (-not (Get-Command mvn -ErrorAction SilentlyContinue)) {
    throw "mvn not found in PATH"
  }
  if (-not (Test-Path $javaDir)) {
    throw "base-java dir not found: $javaDir"
  }
  Info "running base-java tests"
  Push-Location $javaDir
  try {
    WithMavenJvmFlags { mvn -q test }
  }
  finally {
    Pop-Location
  }
  Ok "base-java tests passed"
} else {
  Warn "skip base-java tests"
}

if (-not $SkipPythonTests) {
  if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python not found in PATH"
  }
  if (-not (Test-Path $pythonDir)) {
    throw "glue-python dir not found: $pythonDir"
  }

  Info "running glue-python tests"
  Push-Location $pythonDir
  try {
    python -m unittest discover -s tests -v
  }
  finally {
    Pop-Location
  }
  Ok "glue-python tests passed"
} else {
  Warn "skip glue-python tests"
}

if (-not $SkipRegressionQuality) {
  if (-not (Test-Path $regressionQualityScript)) {
    throw "regression quality script not found: $regressionQualityScript"
  }
  Info "running regression quality checks"
  powershell -ExecutionPolicy Bypass -File $regressionQualityScript
  if ($LASTEXITCODE -ne 0) {
    throw "regression quality checks failed"
  }
  Ok "regression quality checks passed"
} else {
  Warn "skip regression quality checks"
}

if (-not $SkipDesktopUiTests) {
  if (-not (Test-Path $desktopDir)) {
    throw "dify-desktop dir not found: $desktopDir"
  }
  Info "running desktop unit tests"
  Push-Location $desktopDir
  try {
    WithNoDeprecation { npm run test:unit }
  }
  finally {
    Pop-Location
  }
  if ($LASTEXITCODE -ne 0) {
    throw "desktop unit tests failed"
  }
  Ok "desktop unit tests passed"

  if (-not $SkipDesktopStress) {
    Info "running desktop chiplet pool stress check"
    Push-Location $desktopDir
    try {
      $prevStressSeconds = $env:AIWF_CHIPLET_STRESS_SECONDS
      $prevStressConcurrency = $env:AIWF_CHIPLET_STRESS_CONCURRENCY
      $env:AIWF_CHIPLET_STRESS_SECONDS = "10"
      $env:AIWF_CHIPLET_STRESS_CONCURRENCY = "3"
      try {
        WithNoDeprecation { npm run stress:chiplet-pool }
      }
      finally {
        if ($null -eq $prevStressSeconds) { Remove-Item Env:AIWF_CHIPLET_STRESS_SECONDS -ErrorAction SilentlyContinue } else { $env:AIWF_CHIPLET_STRESS_SECONDS = $prevStressSeconds }
        if ($null -eq $prevStressConcurrency) { Remove-Item Env:AIWF_CHIPLET_STRESS_CONCURRENCY -ErrorAction SilentlyContinue } else { $env:AIWF_CHIPLET_STRESS_CONCURRENCY = $prevStressConcurrency }
      }
    }
    finally {
      Pop-Location
    }
    if ($LASTEXITCODE -ne 0) {
      throw "desktop chiplet pool stress check failed"
    }
    Ok "desktop chiplet pool stress check passed"
  } else {
    Warn "skip desktop chiplet pool stress check"
  }

  Info "running desktop workflow UI tests"
  Push-Location $desktopDir
  try {
    WithNoDeprecation { npm run test:workflow-ui }
  }
  finally {
    Pop-Location
  }
  if ($LASTEXITCODE -ne 0) {
    throw "desktop workflow UI tests failed"
  }
  Ok "desktop workflow UI tests passed"
} else {
  Warn "skip desktop workflow UI tests"
}

if (-not $SkipDesktopPackageTests) {
  if (-not (Test-Path $desktopPkgCheckScript)) {
    throw "desktop packaged startup check script not found: $desktopPkgCheckScript"
  }
  if (-not (Test-Path $desktopLitePkgCheckScript)) {
    throw "desktop lite packaged startup check script not found: $desktopLitePkgCheckScript"
  }
  Info "running desktop packaged startup check"
  powershell -ExecutionPolicy Bypass -File $desktopPkgCheckScript -DesktopDir $desktopDir
  if ($LASTEXITCODE -ne 0) {
    throw "desktop packaged startup check failed"
  }
  Ok "desktop packaged startup check passed"

  Info "running desktop lite packaged startup check"
  powershell -ExecutionPolicy Bypass -File $desktopLitePkgCheckScript -DesktopDir $desktopDir
  if ($LASTEXITCODE -ne 0) {
    throw "desktop lite packaged startup check failed"
  }
  Ok "desktop lite packaged startup check passed"
} else {
  Warn "skip desktop packaged startup check"
}

if (-not $SkipSmoke) {
  if (-not (Test-Path $smokeScript)) {
    throw "smoke script not found: $smokeScript"
  }
  Info "running smoke + invalid parquet fallback integration checks"
  powershell -ExecutionPolicy Bypass -File $smokeScript -EnvFile $EnvFile -Owner $Owner -WithInvalidParquetFallbackTest
  if ($LASTEXITCODE -ne 0) {
    throw "smoke/integration checks failed"
  }
  Ok "smoke and integration checks passed"
} else {
  Warn "skip smoke/integration checks"
}

if (-not $SkipContractTests) {
  if (-not (Test-Path $contractRustApiScript)) {
    throw "contract test script not found: $contractRustApiScript"
  }
  Info "running rust api contract tests"
  powershell -ExecutionPolicy Bypass -File $contractRustApiScript -AccelUrl "http://127.0.0.1:18082"
  if ($LASTEXITCODE -ne 0) {
    throw "rust api contract tests failed"
  }
  Ok "rust api contract tests passed"

  Info "running rust otel boot contract test"
  $accelExe = Join-Path $rustDir "target\debug\accel-rust.exe"
  if (-not (Test-Path $accelExe)) {
    Push-Location $rustDir
    try {
      cargo build -q
    }
    finally {
      Pop-Location
    }
  }
  if (-not (Test-Path $accelExe)) {
    throw "accel-rust exe not found for otel contract test: $accelExe"
  }
  $prevHost = $env:AIWF_ACCEL_RUST_HOST
  $prevPort = $env:AIWF_ACCEL_RUST_PORT
  $prevOtel = $env:AIWF_OTEL_EXPORTER_OTLP_ENDPOINT
  $proc = $null
  try {
    $env:AIWF_ACCEL_RUST_HOST = "127.0.0.1"
    $env:AIWF_ACCEL_RUST_PORT = "18093"
    $env:AIWF_OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:1"
    $proc = Start-Process -FilePath $accelExe -WorkingDirectory $rustDir -WindowStyle Hidden -PassThru
    $healthy = $false
    for ($i = 0; $i -lt 60; $i++) {
      Start-Sleep -Milliseconds 250
      try {
        $h = Invoke-RestMethod -Uri "http://127.0.0.1:18093/health" -TimeoutSec 2
        if ($h.ok) { $healthy = $true; break }
      } catch {}
    }
    if (-not $healthy) {
      throw "rust otel boot contract test failed: health not ready at 127.0.0.1:18093"
    }
  }
  finally {
    if ($proc -and -not $proc.HasExited) {
      Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
    if ($null -eq $prevHost) { Remove-Item Env:AIWF_ACCEL_RUST_HOST -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_HOST = $prevHost }
    if ($null -eq $prevPort) { Remove-Item Env:AIWF_ACCEL_RUST_PORT -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_PORT = $prevPort }
    if ($null -eq $prevOtel) { Remove-Item Env:AIWF_OTEL_EXPORTER_OTLP_ENDPOINT -ErrorAction SilentlyContinue } else { $env:AIWF_OTEL_EXPORTER_OTLP_ENDPOINT = $prevOtel }
  }
  Ok "rust otel boot contract test passed"
} else {
  Warn "skip rust api contract tests"
}

if (-not $SkipChaosChecks) {
  if (-not (Test-Path $chaosTaskStoreScript)) {
    throw "chaos script not found: $chaosTaskStoreScript"
  }
  Info "running rust chaos checks"
  powershell -ExecutionPolicy Bypass -File $chaosTaskStoreScript -AccelUrl "http://127.0.0.1:18082"
  if ($LASTEXITCODE -ne 0) {
    throw "rust chaos checks failed"
  }
  Ok "rust chaos checks passed"
} else {
  Warn "skip rust chaos checks"
}

if (-not $SkipRoutingBench) {
  if (-not (Test-Path $desktopDir)) {
    throw "dify-desktop dir not found: $desktopDir"
  }
  Info "running workflow routing benchmark gate"
  Push-Location $desktopDir
  try {
    $prevTrendWindow = $env:AIWF_ROUTE_BENCH_TREND_WINDOW
    $prevTrendMinSamples = $env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES
    $prevTrendMedian = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX
    $prevTrendWorst = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX
    $prevTrendHistory = $env:AIWF_ROUTE_BENCH_HISTORY
    $prevTrendLatest = $env:AIWF_ROUTE_BENCH_LATEST
    $env:AIWF_ROUTE_BENCH_TREND_WINDOW = "7"
    $env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES = "5"
    $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX = "135"
    $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX = "175"
    $routeBenchDir = Join-Path $root "ops\logs\route_bench"
    New-Item -ItemType Directory -Force -Path $routeBenchDir | Out-Null
    $env:AIWF_ROUTE_BENCH_HISTORY = (Join-Path $routeBenchDir "routing_bench_history.jsonl")
    $env:AIWF_ROUTE_BENCH_LATEST = (Join-Path $routeBenchDir "routing_bench_latest.json")
    try {
      WithNoDeprecation { npm run bench:routing }
      if ($LASTEXITCODE -ne 0) {
        throw "workflow routing benchmark gate failed"
      }
      $sampleCount = 0
      if (Test-Path $env:AIWF_ROUTE_BENCH_HISTORY) {
        $sampleCount = @(Get-Content $env:AIWF_ROUTE_BENCH_HISTORY -ErrorAction SilentlyContinue | Where-Object { $_ -match "\S" }).Count
      }
      $minSamples = [Math]::Max(1, [int]($env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES))
      $guard = 0
      while ($sampleCount -lt $minSamples -and $guard -lt 12) {
        $need = $minSamples - $sampleCount
        Warn "routing trend history warm-up: need $need more sample(s) for enforcement"
        WithNoDeprecation { npm run bench:routing }
        if ($LASTEXITCODE -ne 0) {
          throw "workflow routing benchmark gate warm-up failed"
        }
        $sampleCount = @(Get-Content $env:AIWF_ROUTE_BENCH_HISTORY -ErrorAction SilentlyContinue | Where-Object { $_ -match "\S" }).Count
        $guard += 1
      }
    }
    finally {
      if ($null -eq $prevTrendWindow) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_WINDOW -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_WINDOW = $prevTrendWindow }
      if ($null -eq $prevTrendMinSamples) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES = $prevTrendMinSamples }
      if ($null -eq $prevTrendMedian) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX = $prevTrendMedian }
      if ($null -eq $prevTrendWorst) { Remove-Item Env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX = $prevTrendWorst }
      if ($null -eq $prevTrendHistory) { Remove-Item Env:AIWF_ROUTE_BENCH_HISTORY -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_HISTORY = $prevTrendHistory }
      if ($null -eq $prevTrendLatest) { Remove-Item Env:AIWF_ROUTE_BENCH_LATEST -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_LATEST = $prevTrendLatest }
    }
  }
  finally {
    Pop-Location
  }
  if ($LASTEXITCODE -ne 0) {
    throw "workflow routing benchmark gate failed"
  }
  Ok "workflow routing benchmark gate passed"
} else {
  Warn "skip workflow routing benchmark"
}

if (-not $SkipAsyncBench) {
  if (-not (Test-Path $asyncBenchTrendScript)) {
    throw "async bench trend script not found: $asyncBenchTrendScript"
  }
  Info "running rust async benchmark trend gate"
  powershell -ExecutionPolicy Bypass -File $asyncBenchTrendScript -AccelUrl "http://127.0.0.1:18082"
  if ($LASTEXITCODE -ne 0) {
    throw "rust async benchmark trend gate failed"
  }
  Ok "rust async benchmark trend gate passed"
} else {
  Warn "skip rust async benchmark trend gate"
}

if (-not $SkipPostCleanup) {
  if (Test-Path $cleanupScript) {
    Info "running post-ci workspace cleanup"
    powershell -ExecutionPolicy Bypass -File $cleanupScript -RemoveDesktopDist -RemoveDesktopLiteDist -RemoveTmp -RemoveAppsTmp -RemoveOfflineJobs -RemoveBusJobs -KeepLatestBusJobs 3
    if ($LASTEXITCODE -ne 0) {
      throw "post-ci workspace cleanup failed"
    }
    Ok "post-ci workspace cleanup passed"
  }
} else {
  Warn "skip post-ci workspace cleanup"
}

Write-Host ""
Ok "ci check finished"
