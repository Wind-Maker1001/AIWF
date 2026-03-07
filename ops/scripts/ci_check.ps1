param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [ValidateSet("Default","Quick","Full")]
  [string]$CiProfile = "Default",
  [switch]$SkipToolChecks,
  [switch]$SkipDocsChecks,
  [switch]$SkipEncodingChecks,
  [switch]$SkipJavaTests,
  [switch]$SkipRustTests,
  [switch]$SkipPythonTests,
  [switch]$SkipRegressionQuality,
  [switch]$SkipDesktopUiTests,
  [switch]$SkipDesktopRealSampleAcceptance,
  [switch]$SkipDesktopFinanceTemplateAcceptance,
  [switch]$SkipDesktopStress,
  [switch]$SkipDesktopPackageTests,
  [switch]$SkipRoutingBench,
  [switch]$SkipAsyncBench,
  [switch]$SkipRustTransformBenchGate,
  [switch]$SkipRustNewOpsBenchGate,
  [switch]$SkipRegressionBaselineGate,
  [switch]$SkipOpenApiSdkSync,
  [switch]$SkipSecretScan,
  [switch]$SkipContractTests,
  [switch]$SkipChaosChecks,
  [switch]$SkipSmoke,
  [switch]$SkipSqlConnectivityGate,
  [switch]$SkipNativeWinuiSmoke
  ,
  [switch]$SkipPostCleanup
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function ApplyCiProfile([string]$ProfileName, [hashtable]$BoundParams) {
  $normalized = "default"
  if (-not [string]::IsNullOrWhiteSpace($ProfileName)) {
    $normalized = $ProfileName.Trim().ToLowerInvariant()
  }
  if ($normalized -eq "default") { return }

  Info "using ci profile: $normalized"
  if ($normalized -eq "full") { return }

  if ($normalized -ne "quick") {
    throw "unsupported ci profile: $ProfileName"
  }

  $quickSkipParams = @(
    "SkipRegressionQuality",
    "SkipDesktopRealSampleAcceptance",
    "SkipDesktopFinanceTemplateAcceptance",
    "SkipDesktopStress",
    "SkipRoutingBench",
    "SkipAsyncBench",
    "SkipRustTransformBenchGate",
    "SkipRustNewOpsBenchGate",
    "SkipContractTests",
    "SkipChaosChecks",
    "SkipSmoke",
    "SkipNativeWinuiSmoke"
  )

  $applied = @()
  foreach ($paramName in $quickSkipParams) {
    if ($BoundParams.ContainsKey($paramName)) { continue }
    Set-Variable -Name $paramName -Value $true -Scope Script
    $applied += $paramName
  }

  if ($applied.Count -gt 0) {
    $labels = $applied | ForEach-Object { ($_ -replace "^Skip", "") }
    Info ("quick profile auto-skips: {0}" -f ($labels -join ", "))
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

function Wait-AccelRustHealthy([string]$Url, [int]$MaxRetries = 80, [int]$DelayMs = 250) {
  for ($i = 0; $i -lt $MaxRetries; $i++) {
    try {
      $h = Invoke-RestMethod -Uri "$Url/health" -TimeoutSec 2
      if ($h.ok) { return $true }
    } catch {}
    Start-Sleep -Milliseconds $DelayMs
  }
  return $false
}

function Ensure-AccelRustService([string]$RustDir, [string]$AccelUrl = "http://127.0.0.1:18082") {
  try {
    $healthy = Invoke-RestMethod -Uri "$AccelUrl/health" -TimeoutSec 2
    if ($healthy.ok) {
      return @{
        started = $false
        proc = $null
        prevHost = $null
        prevPort = $null
      }
    }
  } catch {}

  $accelExe = Join-Path $RustDir "target\debug\accel-rust.exe"
  if (-not (Test-Path $accelExe)) {
    Push-Location $RustDir
    try {
      cargo build -q
    }
    finally {
      Pop-Location
    }
  }
  if (-not (Test-Path $accelExe)) {
    throw "accel-rust exe not found for local bootstrap: $accelExe"
  }

  $uri = [Uri]$AccelUrl
  $bindHost = if ([string]::IsNullOrWhiteSpace($uri.Host)) { "127.0.0.1" } else { $uri.Host }
  $port = if ($uri.Port -gt 0) { "$($uri.Port)" } else { "18082" }
  $prevHost = $env:AIWF_ACCEL_RUST_HOST
  $prevPort = $env:AIWF_ACCEL_RUST_PORT
  $env:AIWF_ACCEL_RUST_HOST = $bindHost
  $env:AIWF_ACCEL_RUST_PORT = $port
  $proc = Start-Process -FilePath $accelExe -WorkingDirectory $RustDir -WindowStyle Hidden -PassThru
  if (-not (Wait-AccelRustHealthy -Url $AccelUrl -MaxRetries 100 -DelayMs 250)) {
    if ($proc -and -not $proc.HasExited) {
      Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
    if ($null -eq $prevHost) { Remove-Item Env:AIWF_ACCEL_RUST_HOST -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_HOST = $prevHost }
    if ($null -eq $prevPort) { Remove-Item Env:AIWF_ACCEL_RUST_PORT -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_PORT = $prevPort }
    throw "failed to bootstrap accel-rust at $AccelUrl"
  }

  return @{
    started = $true
    proc = $proc
    prevHost = $prevHost
    prevPort = $prevPort
  }
}

function Stop-AccelRustService($State) {
  if ($null -eq $State) { return }
  if ($State.started -and $State.proc -and -not $State.proc.HasExited) {
    Stop-Process -Id $State.proc.Id -Force -ErrorAction SilentlyContinue
  }
  if ($State.started) {
    if ($null -eq $State.prevHost) { Remove-Item Env:AIWF_ACCEL_RUST_HOST -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_HOST = $State.prevHost }
    if ($null -eq $State.prevPort) { Remove-Item Env:AIWF_ACCEL_RUST_PORT -ErrorAction SilentlyContinue } else { $env:AIWF_ACCEL_RUST_PORT = $State.prevPort }
  }
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
ApplyCiProfile -ProfileName $CiProfile -BoundParams $PSBoundParameters
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
$releaseEvidenceCheckScript = Join-Path $PSScriptRoot "check_release_evidence.ps1"
$encodingCheckScript = Join-Path $PSScriptRoot "check_encoding_health.ps1"
$desktopPkgCheckScript = Join-Path $PSScriptRoot "check_desktop_packaged_startup.ps1"
$desktopLitePkgCheckScript = Join-Path $PSScriptRoot "check_desktop_lite_packaged_startup.ps1"
$desktopRealSampleScript = Join-Path $PSScriptRoot "acceptance_desktop_real_sample.ps1"
$desktopFinanceTemplateScript = Join-Path $PSScriptRoot "acceptance_desktop_finance_template.ps1"
$regressionQualityScript = Join-Path $PSScriptRoot "run_regression_quality.ps1"
$regressionBaselineScript = Join-Path $PSScriptRoot "check_regression_baseline.ps1"
$asyncBenchTrendScript = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$rustTransformBenchGateScript = Join-Path $PSScriptRoot "check_rust_transform_bench_gate.ps1"
$rustNewOpsBenchGateScript = Join-Path $PSScriptRoot "check_rust_new_ops_bench_gate.ps1"
$openApiSdkSyncScript = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$secretScanScript = Join-Path $PSScriptRoot "secret_scan.ps1"
$contractRustApiScript = Join-Path $PSScriptRoot "contract_test_rust_api.ps1"
$chaosTaskStoreScript = Join-Path $PSScriptRoot "chaos_task_store.ps1"
$sqlConnectivityScript = Join-Path $PSScriptRoot "check_sql_connectivity.ps1"
$nativeWinuiSmokeScript = Join-Path $PSScriptRoot "check_native_winui_smoke.ps1"
$cleanupScript = Join-Path $PSScriptRoot "clean_workspace_artifacts.ps1"
$restartServicesScript = Join-Path $PSScriptRoot "restart_services.ps1"
$accelServiceState = $null

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
  if (Test-Path $releaseEvidenceCheckScript) {
    Info "running release evidence checks"
    powershell -ExecutionPolicy Bypass -File $releaseEvidenceCheckScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      throw "release evidence checks failed"
    }
    Ok "release evidence checks passed"
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

if (-not $SkipSqlConnectivityGate) {
  if (-not (Test-Path $sqlConnectivityScript)) {
    throw "sql connectivity gate script not found: $sqlConnectivityScript"
  }
  Info "running SQL connectivity gate"
  powershell -ExecutionPolicy Bypass -File $sqlConnectivityScript -EnvFile $EnvFile -SkipWhenTaskStoreNotSql
  if ($LASTEXITCODE -ne 0) {
    throw "SQL connectivity gate failed"
  }
  Ok "SQL connectivity gate passed"
} else {
  Warn "skip SQL connectivity gate"
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
  if (-not $SkipRegressionBaselineGate) {
    if (-not (Test-Path $regressionBaselineScript)) {
      throw "regression baseline gate script not found: $regressionBaselineScript"
    }
    Info "running regression baseline gate"
    powershell -ExecutionPolicy Bypass -File $regressionBaselineScript
    if ($LASTEXITCODE -ne 0) {
      throw "regression baseline gate failed"
    }
    Ok "regression baseline gate passed"
  } else {
    Warn "skip regression baseline gate"
  }
} else {
  Warn "skip regression quality checks"
}

if (-not $SkipDesktopUiTests) {
  if (-not (Test-Path $desktopDir)) {
    throw "dify-desktop dir not found: $desktopDir"
  }
  $desktopDepsReady = `
    (Test-Path (Join-Path $desktopDir "node_modules\exceljs\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\iconv-lite\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\@playwright\test\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\electron-builder\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\app-builder-bin\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\electron\package.json")) -and `
    (Test-Path (Join-Path $desktopDir "node_modules\electron\dist\electron.exe"))

  if (-not $desktopDepsReady) {
    $desktopElectronInstall = Join-Path $desktopDir "node_modules\electron\install.js"
    $desktopElectronExe = Join-Path $desktopDir "node_modules\electron\dist\electron.exe"
    $desktopAppBuilderBin = Join-Path $desktopDir "node_modules\app-builder-bin\package.json"
    $desktopPlaywrightPkg = Join-Path $desktopDir "node_modules\@playwright\test\package.json"
    $desktopElectronBuilderPkg = Join-Path $desktopDir "node_modules\electron-builder\package.json"

    if ((Test-Path $desktopPlaywrightPkg) -and (Test-Path $desktopElectronBuilderPkg)) {
      if (-not (Test-Path $desktopAppBuilderBin)) {
        Info "repairing desktop app-builder-bin dependency"
        Push-Location $desktopDir
        try {
          npm install --no-save --prefer-offline --no-audit --fund=false --progress=false app-builder-bin@5.0.0-alpha.12
        }
        finally {
          Pop-Location
        }
        if ($LASTEXITCODE -ne 0) {
          Warn "desktop app-builder-bin repair failed"
        } else {
          Ok "desktop app-builder-bin repaired"
        }
      }

      if ((Test-Path $desktopElectronInstall) -and (-not (Test-Path $desktopElectronExe))) {
        Info "repairing desktop electron runtime"
        Push-Location $desktopDir
        try {
          node .\node_modules\electron\install.js
        }
        finally {
          Pop-Location
        }
        if ($LASTEXITCODE -ne 0) {
          Warn "desktop electron runtime repair failed"
        } else {
          Ok "desktop electron runtime repaired"
        }
      }
    }

    $desktopDepsReady = `
      (Test-Path (Join-Path $desktopDir "node_modules\exceljs\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\iconv-lite\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\@playwright\test\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\electron-builder\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\app-builder-bin\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\electron\package.json")) -and `
      (Test-Path (Join-Path $desktopDir "node_modules\electron\dist\electron.exe"))
  }

  if (-not $desktopDepsReady) {
    Info "installing desktop dependencies (including devDependencies)"
    $nodeModulesDir = Join-Path $desktopDir "node_modules"
    if (Test-Path $nodeModulesDir) {
      $staleDir = Join-Path $desktopDir ("node_modules_stale_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
      try {
        Rename-Item $nodeModulesDir $staleDir -ErrorAction Stop
        Warn "desktop node_modules moved aside: $staleDir"
      }
      catch {
        Warn "desktop node_modules rename skipped: $($_.Exception.Message)"
      }
    }

    $installOk = $false
    for ($attempt = 1; $attempt -le 3; $attempt++) {
      Push-Location $desktopDir
      try {
        $env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1"
        try {
          npm ci --include=dev --prefer-offline --no-audit --fund=false --progress=false
        }
        finally {
          Remove-Item Env:PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD -ErrorAction SilentlyContinue
        }
      }
      finally {
        Pop-Location
      }
      if ($LASTEXITCODE -eq 0) {
        $installOk = $true
        break
      }
      if ($attempt -lt 3) {
        Warn "desktop dependency install attempt $attempt failed, retrying..."
        Start-Sleep -Seconds (5 * $attempt)
      }
    }
    if (-not $installOk) {
      throw "desktop dependency install failed"
    }
    Ok "desktop dependencies installed"
  }
  else {
    Ok "desktop dependencies already present"
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

  if (-not $SkipDesktopRealSampleAcceptance) {
    if (-not (Test-Path $desktopRealSampleScript)) {
      throw "desktop real sample acceptance script not found: $desktopRealSampleScript"
    }
    Info "running desktop real sample acceptance"
    powershell -ExecutionPolicy Bypass -File $desktopRealSampleScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      throw "desktop real sample acceptance failed"
    }
    Ok "desktop real sample acceptance passed"
  } else {
    Warn "skip desktop real sample acceptance"
  }

  if (-not $SkipDesktopFinanceTemplateAcceptance) {
    if (-not (Test-Path $desktopFinanceTemplateScript)) {
      throw "desktop finance template acceptance script not found: $desktopFinanceTemplateScript"
    }
    Info "running desktop finance template acceptance"
    powershell -ExecutionPolicy Bypass -File $desktopFinanceTemplateScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      throw "desktop finance template acceptance failed"
    }
    Ok "desktop finance template acceptance passed"
  } else {
    Warn "skip desktop finance template acceptance"
  }
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
  $isCi = [string]::Equals($env:CI, "true", [System.StringComparison]::OrdinalIgnoreCase) -or `
    [string]::Equals($env:GITHUB_ACTIONS, "true", [System.StringComparison]::OrdinalIgnoreCase)
  $bootstrapTimeoutSeconds = if ($isCi) { 600 } else { 90 }
  $runSmokeChecks = $true
  if (Test-Path $restartServicesScript) {
    Info "ensuring base/glue/accel services are healthy before smoke"
    powershell -ExecutionPolicy Bypass -File $restartServicesScript -EnvFile $EnvFile -TimeoutSeconds $bootstrapTimeoutSeconds
    if ($LASTEXITCODE -ne 0) {
      if ($isCi) {
        throw "service bootstrap before smoke failed"
      }
      Warn "service bootstrap before smoke failed in local mode; skip smoke/integration checks"
      $runSmokeChecks = $false
    } else {
      Ok "service bootstrap before smoke passed"
    }
  } else {
    Warn "restart services script not found, continue without bootstrap: $restartServicesScript"
  }
  if ($runSmokeChecks) {
    Info "running smoke + invalid parquet fallback integration checks"
    powershell -ExecutionPolicy Bypass -File $smokeScript -EnvFile $EnvFile -Owner $Owner -WithInvalidParquetFallbackTest
    if ($LASTEXITCODE -ne 0) {
      if ($isCi) {
        throw "smoke/integration checks failed"
      }
      Warn "smoke/integration checks failed in local mode; continue"
    } else {
      Ok "smoke and integration checks passed"
    }
  } else {
    Warn "skip smoke/integration checks because dependent services are unavailable in local mode"
  }
} else {
  Warn "skip smoke/integration checks"
}

$needsAccelRustService = (-not $SkipContractTests) -or (-not $SkipChaosChecks) -or (-not $SkipAsyncBench) -or (-not $SkipRustTransformBenchGate)
if ($needsAccelRustService) {
  Info "ensuring accel-rust service is available on 127.0.0.1:18082"
  $accelServiceState = Ensure-AccelRustService -RustDir $rustDir -AccelUrl "http://127.0.0.1:18082"
  if ($accelServiceState.started) {
    Ok "accel-rust service bootstrapped"
  } else {
    Ok "accel-rust service already available"
  }
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
    $prevMaxPerEdge = $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE
    $prevMaxWorstPerEdge = $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE
    $prevMaxFallbackRatio = $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO
    $prevMaxFixedFallbackRatio = $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO
    $prevTrendMedian = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX
    $prevTrendWorst = $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX
    $prevTrendHistory = $env:AIWF_ROUTE_BENCH_HISTORY
    $prevTrendLatest = $env:AIWF_ROUTE_BENCH_LATEST
    $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE = "130"
    $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE = "170"
    $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO = "0.50"
    $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO = "0.50"
    $env:AIWF_ROUTE_BENCH_TREND_WINDOW = "7"
    $env:AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES = "5"
    $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX = "120"
    $env:AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX = "160"
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
      if ($null -eq $prevMaxPerEdge) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE = $prevMaxPerEdge }
      if ($null -eq $prevMaxWorstPerEdge) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE = $prevMaxWorstPerEdge }
      if ($null -eq $prevMaxFallbackRatio) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO = $prevMaxFallbackRatio }
      if ($null -eq $prevMaxFixedFallbackRatio) { Remove-Item Env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO -ErrorAction SilentlyContinue } else { $env:AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO = $prevMaxFixedFallbackRatio }
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

if (-not $SkipRustTransformBenchGate) {
  if (-not (Test-Path $rustTransformBenchGateScript)) {
    throw "rust transform benchmark gate script not found: $rustTransformBenchGateScript"
  }
  Info "running rust transform benchmark gate"
  powershell -ExecutionPolicy Bypass -File $rustTransformBenchGateScript -AccelUrl "http://127.0.0.1:18082" -Rows 120000 -Runs 4 -Warmup 1 -MinSpeedup 1.01 -MinArrowSpeedup 0.90 -GateToleranceSpeedup 0.03 -MaxNoisyRegressionMs 15 -EnforceArrowAlways
  if ($LASTEXITCODE -ne 0) {
    throw "rust transform benchmark gate failed"
  }
  Ok "rust transform benchmark gate passed"
} else {
  Warn "skip rust transform benchmark gate"
}

if (-not $SkipRustNewOpsBenchGate) {
  if (-not (Test-Path $rustNewOpsBenchGateScript)) {
    throw "rust new-ops benchmark gate script not found: $rustNewOpsBenchGateScript"
  }
  Info "running rust new-ops benchmark gate"
  powershell -ExecutionPolicy Bypass -File $rustNewOpsBenchGateScript -RustDir $rustDir -MaxColumnarMs 2500 -MaxStreamWindowMs 2500 -MaxSketchMs 2500
  if ($LASTEXITCODE -ne 0) {
    throw "rust new-ops benchmark gate failed"
  }
  Ok "rust new-ops benchmark gate passed"
} else {
  Warn "skip rust new-ops benchmark gate"
}

if (-not $SkipNativeWinuiSmoke) {
  if (-not (Test-Path $nativeWinuiSmokeScript)) {
    throw "native winui smoke script not found: $nativeWinuiSmokeScript"
  }
  $isCi = [string]::Equals($env:CI, "true", [System.StringComparison]::OrdinalIgnoreCase) -or `
    [string]::Equals($env:GITHUB_ACTIONS, "true", [System.StringComparison]::OrdinalIgnoreCase)
  if ($isCi) {
    Warn "skip native winui smoke in CI environment"
  } else {
    Info "running native winui smoke check"
    powershell -ExecutionPolicy Bypass -File $nativeWinuiSmokeScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      throw "native winui smoke check failed"
    }
    Ok "native winui smoke check passed"
  }
} else {
  Warn "skip native winui smoke check"
}

Stop-AccelRustService $accelServiceState

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
