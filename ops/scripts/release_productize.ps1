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
  [switch]$SkipWorkflowContractSyncGate,
  [switch]$SkipGovernanceControlPlaneBoundaryGate,
  [switch]$SkipOperatorCatalogSyncGate,
  [switch]$SkipFallbackGovernanceGate,
  [switch]$SkipGovernanceStoreSchemaVersionsGate,
  [switch]$SkipLocalWorkflowStoreSchemaVersionsGate,
  [switch]$SkipTemplatePackContractSyncGate,
  [switch]$SkipLocalTemplateStorageContractSyncGate,
  [switch]$SkipOfflineTemplateCatalogSyncGate,
  [int]$RustBenchRows = 100000,
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
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
. (Join-Path $PSScriptRoot "governance_capability_export_support.ps1")
function Read-FrontendVerificationEvidence([string]$Path, [string]$Role, [string]$Frontend) {
  $summary = [ordered]@{
    frontend_role = $Role
    frontend = $Frontend
    evidence_path = $Path
    exists = $false
    overall_status = "missing"
    generated_at = ""
    profile = ""
    checks = [ordered]@{}
  }
  if (-not (Test-Path $Path)) {
    return $summary
  }
  try {
    $raw = Get-Content -Raw -Encoding UTF8 $Path | ConvertFrom-Json
    $summary.exists = $true
    $summary.overall_status = [string]($raw.overall_status)
    $summary.generated_at = [string]($raw.generated_at)
    $summary.profile = [string]($raw.profile)
    $summary.checks = if ($raw.checks) { $raw.checks } else { [ordered]@{} }
  } catch {
    $summary.exists = $true
    $summary.overall_status = "unreadable"
  }
  return $summary
}
function Get-FrontendVerificationSummary([string]$Root) {
  $dir = Join-Path $Root "ops\logs\frontend_verification"
  $primaryPath = Join-Path $dir "frontend_primary_verification_latest.json"
  $compatibilityPath = Join-Path $dir "frontend_compatibility_verification_latest.json"
  return [ordered]@{
    primary = Read-FrontendVerificationEvidence -Path $primaryPath -Role "primary" -Frontend "winui"
    compatibility = Read-FrontendVerificationEvidence -Path $compatibilityPath -Role "compatibility" -Frontend "electron"
  }
}
function Get-ArchitectureScorecardSummary([string]$Root) {
  $dir = Join-Path $Root "ops\logs\architecture"
  $jsonPath = Join-Path $dir "architecture_scorecard_release_ready_latest.json"
  $mdPath = Join-Path $dir "architecture_scorecard_release_ready_latest.md"
  $summary = [ordered]@{
    evidence_path = $jsonPath
    markdown_path = $mdPath
    exists = $false
    overall_status = "missing"
    generated_at = ""
    profile = ""
    boundaries = [ordered]@{}
  }
  if (-not (Test-Path $jsonPath)) {
    return $summary
  }
  try {
    $raw = Get-Content -Raw -Encoding UTF8 $jsonPath | ConvertFrom-Json
    $summary.exists = $true
    $summary.overall_status = [string]($raw.overall_status)
    $summary.generated_at = [string]($raw.generated_at)
    $summary.profile = [string]($raw.profile)
    $summary.boundaries = if ($raw.boundaries) { $raw.boundaries } else { [ordered]@{} }
  } catch {
    $summary.exists = $true
    $summary.overall_status = "unreadable"
  }
  return $summary
}
function Assert-ArchitectureScorecardReleaseReady($Summary) {
  if (-not $Summary.exists) {
    throw "release blocked by architecture scorecard gate: missing release-ready scorecard. Run ci_check.ps1 -CiProfile Quick and ci_check.ps1 -CiProfile Compatibility first."
  }
  if ([string]$Summary.overall_status -ne "passed") {
    throw ("release blocked by architecture scorecard gate: overall_status={0}. Refresh ci_check.ps1 -CiProfile Quick and ci_check.ps1 -CiProfile Compatibility before release." -f ([string]$Summary.overall_status))
  }
}
function Get-SidecarReportSummary([string]$Root, [string]$Name) {
  $dir = Join-Path $Root "ops\logs\regression"
  $jsonPath = Join-Path $dir ("{0}.json" -f $Name)
  $summary = [ordered]@{
    evidence_path = $jsonPath
    exists = $false
    ok = $false
    generated_at = ""
    failed = @()
    skipped = @()
  }
  if (-not (Test-Path $jsonPath)) {
    return $summary
  }
  try {
    $raw = Get-Content -Raw -Encoding UTF8 $jsonPath | ConvertFrom-Json
    $summary.exists = $true
    $summary.ok = [bool]$raw.ok
    $summary.generated_at = [string]($raw.generated_at)
    if ($raw.PSObject.Properties.Name -contains "failed") {
      $summary.failed = @($raw.failed | ForEach-Object { [string]$_ })
    }
    if ($raw.PSObject.Properties.Name -contains "skipped") {
      $summary.skipped = @($raw.skipped | ForEach-Object { [string]$_ })
    }
  } catch {
    $summary.exists = $true
    $summary.ok = $false
  }
  return $summary
}
function Assert-SidecarReportReady($Summary, [string]$Label, [switch]$RequireNoSkipped) {
  if (-not $Summary.exists) {
    throw ("release blocked by {0}: missing report {1}. Run the sidecar regression verification first." -f $Label, [string]$Summary.evidence_path)
  }
  if (-not $Summary.ok) {
    throw ("release blocked by {0}: report not ok ({1})" -f $Label, [string]$Summary.evidence_path)
  }
  if ($RequireNoSkipped -and @($Summary.skipped).Count -gt 0) {
    throw ("release blocked by {0}: skipped entries present ({1})" -f $Label, (@($Summary.skipped) -join ", "))
  }
}

Warn "release_productize.ps1 is the legacy Electron compatibility release path. Use release_frontend_productize.ps1 for the primary WinUI frontend."

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$pkg = Join-Path $PSScriptRoot "package_offline_bundle.ps1"
$zip = Join-Path $PSScriptRoot "zip_offline_bundle.ps1"
$asyncTrend = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$rustTransformBenchGate = Join-Path $PSScriptRoot "check_rust_transform_bench_gate.ps1"
$sqlConnectivityGate = Join-Path $PSScriptRoot "check_sql_connectivity.ps1"
$openApiSdkSyncGate = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$workflowContractSyncGate = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceCapabilityExportScript = Join-Path $PSScriptRoot "export_governance_capabilities.ps1"
$governanceControlPlaneBoundaryGate = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$operatorCatalogSyncGate = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$fallbackGovernanceGate = Join-Path $PSScriptRoot "check_fallback_governance.ps1"
$governanceStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_governance_store_schema_versions.ps1"
$localWorkflowStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_local_workflow_store_schema_versions.ps1"
$templatePackContractSyncGate = Join-Path $PSScriptRoot "check_template_pack_contract_sync.ps1"
$localTemplateStorageContractSyncGate = Join-Path $PSScriptRoot "check_local_template_storage_contract_sync.ps1"
$offlineTemplateCatalogSyncGate = Join-Path $PSScriptRoot "check_offline_template_catalog_sync.ps1"
$openApiSdkSyncGateStatus = "skipped"
$openApiSdkSyncGateCheckedAt = ""
$openApiSdkSyncGateError = ""
$workflowContractSyncGateStatus = "skipped"
$workflowContractSyncGateCheckedAt = ""
$workflowContractSyncGateError = ""
$governanceCapabilityExportStatus = "skipped"
$governanceCapabilityExportCheckedAt = ""
$governanceCapabilityExportError = ""
$governanceControlPlaneBoundaryGateStatus = "skipped"
$governanceControlPlaneBoundaryGateCheckedAt = ""
$governanceControlPlaneBoundaryGateError = ""
$operatorCatalogSyncGateStatus = "skipped"
$operatorCatalogSyncGateCheckedAt = ""
$operatorCatalogSyncGateError = ""
$fallbackGovernanceGateStatus = "skipped"
$fallbackGovernanceGateCheckedAt = ""
$fallbackGovernanceGateError = ""
$governanceStoreSchemaVersionsGateStatus = "skipped"
$governanceStoreSchemaVersionsGateCheckedAt = ""
$governanceStoreSchemaVersionsGateError = ""
$localWorkflowStoreSchemaVersionsGateStatus = "skipped"
$localWorkflowStoreSchemaVersionsGateCheckedAt = ""
$localWorkflowStoreSchemaVersionsGateError = ""
$templatePackContractSyncGateStatus = "skipped"
$templatePackContractSyncGateCheckedAt = ""
$templatePackContractSyncGateError = ""
$localTemplateStorageContractSyncGateStatus = "skipped"
$localTemplateStorageContractSyncGateCheckedAt = ""
$localTemplateStorageContractSyncGateError = ""
$offlineTemplateCatalogSyncGateStatus = "skipped"
$offlineTemplateCatalogSyncGateCheckedAt = ""
$offlineTemplateCatalogSyncGateError = ""
$architectureScorecardGateStatus = "skipped"
$architectureScorecardGateCheckedAt = ""
$architectureScorecardGateError = ""
$sidecarRegressionGateStatus = "skipped"
$sidecarRegressionGateCheckedAt = ""
$sidecarRegressionGateError = ""
$sidecarConsistencyGateStatus = "skipped"
$sidecarConsistencyGateCheckedAt = ""
$sidecarConsistencyGateError = ""
$outRoot = Join-Path $root "release"
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
$architectureScorecardSummary = Get-ArchitectureScorecardSummary -Root $root
$sidecarRegressionSummary = Get-SidecarReportSummary -Root $root -Name "sidecar_regression_quality_report"
$sidecarConsistencySummary = Get-SidecarReportSummary -Root $root -Name "sidecar_python_rust_consistency_report"

Info "running architecture scorecard release gate"
try {
  Assert-ArchitectureScorecardReleaseReady $architectureScorecardSummary
  $architectureScorecardGateStatus = "passed"
  $architectureScorecardGateCheckedAt = (Get-Date).ToString("s")
  Ok "architecture scorecard release gate passed"
} catch {
  $architectureScorecardGateStatus = "failed"
  $architectureScorecardGateCheckedAt = (Get-Date).ToString("s")
  $architectureScorecardGateError = [string]$_.Exception.Message
  throw
}

Info "running sidecar regression release gate"
try {
  Assert-SidecarReportReady $sidecarRegressionSummary "sidecar regression release gate"
  $sidecarRegressionGateStatus = "passed"
  $sidecarRegressionGateCheckedAt = (Get-Date).ToString("s")
  Ok "sidecar regression release gate passed"
} catch {
  $sidecarRegressionGateStatus = "failed"
  $sidecarRegressionGateCheckedAt = (Get-Date).ToString("s")
  $sidecarRegressionGateError = [string]$_.Exception.Message
  throw
}

Info "running sidecar python/rust consistency release gate"
try {
  Assert-SidecarReportReady $sidecarConsistencySummary "sidecar python/rust consistency release gate" -RequireNoSkipped
  $sidecarConsistencyGateStatus = "passed"
  $sidecarConsistencyGateCheckedAt = (Get-Date).ToString("s")
  Ok "sidecar python/rust consistency release gate passed"
} catch {
  $sidecarConsistencyGateStatus = "failed"
  $sidecarConsistencyGateCheckedAt = (Get-Date).ToString("s")
  $sidecarConsistencyGateError = [string]$_.Exception.Message
  throw
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

if (-not $SkipWorkflowContractSyncGate) {
  if (-not (Test-Path $workflowContractSyncGate)) { throw "workflow contract sync gate script missing: $workflowContractSyncGate" }
  Info "running workflow contract sync release gate"
  powershell -ExecutionPolicy Bypass -File $workflowContractSyncGate
  $workflowContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $workflowContractSyncGateStatus = "failed"
    $workflowContractSyncGateError = "check_workflow_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by workflow contract sync gate"
  }
  $workflowContractSyncGateStatus = "passed"
  Ok "workflow contract sync release gate passed"
} else {
  $workflowContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip workflow contract sync release gate" -ForegroundColor Yellow
}

$governanceCapabilityExportResult = Invoke-GovernanceCapabilityExportStep -ScriptPath $governanceCapabilityExportScript -FailureScope "release"
$governanceCapabilityExportCheckedAt = [string]$governanceCapabilityExportResult.checked_at
if (-not $governanceCapabilityExportResult.ok) {
  $governanceCapabilityExportStatus = "failed"
  $governanceCapabilityExportError = [string]$governanceCapabilityExportResult.error
  throw [string]$governanceCapabilityExportResult.failure_message
}
$governanceCapabilityExportStatus = "passed"

if (-not $SkipGovernanceControlPlaneBoundaryGate) {
  if (-not (Test-Path $governanceControlPlaneBoundaryGate)) { throw "governance control plane boundary gate script missing: $governanceControlPlaneBoundaryGate" }
  Info "running governance control plane boundary release gate"
  powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryGate
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $governanceControlPlaneBoundaryGateStatus = "failed"
    $governanceControlPlaneBoundaryGateError = "check_governance_control_plane_boundary.ps1 exit code $LASTEXITCODE"
    throw "release blocked by governance control plane boundary gate"
  }
  $governanceControlPlaneBoundaryGateStatus = "passed"
  Ok "governance control plane boundary release gate passed"
} else {
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance control plane boundary release gate" -ForegroundColor Yellow
}

if (-not $SkipOperatorCatalogSyncGate) {
  if (-not (Test-Path $operatorCatalogSyncGate)) { throw "operator catalog sync gate script missing: $operatorCatalogSyncGate" }
  Info "running operator catalog sync release gate"
  powershell -ExecutionPolicy Bypass -File $operatorCatalogSyncGate
  $operatorCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $operatorCatalogSyncGateStatus = "failed"
    $operatorCatalogSyncGateError = "check_operator_catalog_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by operator catalog sync gate"
  }
  $operatorCatalogSyncGateStatus = "passed"
  Ok "operator catalog sync release gate passed"
} else {
  $operatorCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip operator catalog sync release gate" -ForegroundColor Yellow
}

if (-not $SkipFallbackGovernanceGate) {
  if (-not (Test-Path $fallbackGovernanceGate)) { throw "fallback governance gate script missing: $fallbackGovernanceGate" }
  Info "running fallback governance release gate"
  powershell -ExecutionPolicy Bypass -File $fallbackGovernanceGate
  $fallbackGovernanceGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $fallbackGovernanceGateStatus = "failed"
    $fallbackGovernanceGateError = "check_fallback_governance.ps1 exit code $LASTEXITCODE"
    throw "release blocked by fallback governance gate"
  }
  $fallbackGovernanceGateStatus = "passed"
  Ok "fallback governance release gate passed"
} else {
  $fallbackGovernanceGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip fallback governance release gate" -ForegroundColor Yellow
}

if (-not $SkipGovernanceStoreSchemaVersionsGate) {
  if (-not (Test-Path $governanceStoreSchemaVersionsGate)) { throw "governance store schema version gate script missing: $governanceStoreSchemaVersionsGate" }
  Info "running governance store schema version release gate"
  powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionsGate
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $governanceStoreSchemaVersionsGateStatus = "failed"
    $governanceStoreSchemaVersionsGateError = "check_governance_store_schema_versions.ps1 exit code $LASTEXITCODE"
    throw "release blocked by governance store schema version gate"
  }
  $governanceStoreSchemaVersionsGateStatus = "passed"
  Ok "governance store schema version release gate passed"
} else {
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance store schema version release gate" -ForegroundColor Yellow
}

if (-not $SkipLocalWorkflowStoreSchemaVersionsGate) {
  if (-not (Test-Path $localWorkflowStoreSchemaVersionsGate)) { throw "local workflow store schema version gate script missing: $localWorkflowStoreSchemaVersionsGate" }
  Info "running local workflow store schema version release gate"
  powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionsGate
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $localWorkflowStoreSchemaVersionsGateStatus = "failed"
    $localWorkflowStoreSchemaVersionsGateError = "check_local_workflow_store_schema_versions.ps1 exit code $LASTEXITCODE"
    throw "release blocked by local workflow store schema version gate"
  }
  $localWorkflowStoreSchemaVersionsGateStatus = "passed"
  Ok "local workflow store schema version release gate passed"
} else {
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local workflow store schema version release gate" -ForegroundColor Yellow
}

if (-not $SkipTemplatePackContractSyncGate) {
  if (-not (Test-Path $templatePackContractSyncGate)) { throw "template pack contract sync gate script missing: $templatePackContractSyncGate" }
  Info "running template pack contract release gate"
  powershell -ExecutionPolicy Bypass -File $templatePackContractSyncGate
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $templatePackContractSyncGateStatus = "failed"
    $templatePackContractSyncGateError = "check_template_pack_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by template pack contract gate"
  }
  $templatePackContractSyncGateStatus = "passed"
  Ok "template pack contract release gate passed"
} else {
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip template pack contract release gate" -ForegroundColor Yellow
}

if (-not $SkipLocalTemplateStorageContractSyncGate) {
  if (-not (Test-Path $localTemplateStorageContractSyncGate)) { throw "local template storage contract gate script missing: $localTemplateStorageContractSyncGate" }
  Info "running local template storage contract release gate"
  powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncGate
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $localTemplateStorageContractSyncGateStatus = "failed"
    $localTemplateStorageContractSyncGateError = "check_local_template_storage_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by local template storage contract gate"
  }
  $localTemplateStorageContractSyncGateStatus = "passed"
  Ok "local template storage contract release gate passed"
} else {
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local template storage contract release gate" -ForegroundColor Yellow
}

if (-not $SkipOfflineTemplateCatalogSyncGate) {
  if (-not (Test-Path $offlineTemplateCatalogSyncGate)) { throw "offline template catalog sync gate script missing: $offlineTemplateCatalogSyncGate" }
  Info "running offline template catalog release gate"
  powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncGate
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $offlineTemplateCatalogSyncGateStatus = "failed"
    $offlineTemplateCatalogSyncGateError = "check_offline_template_catalog_sync.ps1 exit code $LASTEXITCODE"
    throw "release blocked by offline template catalog gate"
  }
  $offlineTemplateCatalogSyncGateStatus = "passed"
  Ok "offline template catalog release gate passed"
} else {
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip offline template catalog release gate" -ForegroundColor Yellow
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
    "-MinArrowSpeedup", "$RustBenchMinArrowSpeedup"
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
    SkipWorkflowContractSyncGate = $true
    SkipGovernanceControlPlaneBoundaryGate = $true
    SkipOperatorCatalogSyncGate = $true
    SkipFallbackGovernanceGate = $true
    SkipGovernanceStoreSchemaVersionsGate = $true
    SkipLocalWorkflowStoreSchemaVersionsGate = $true
    SkipTemplatePackContractSyncGate = $true
    SkipLocalTemplateStorageContractSyncGate = $true
    SkipOfflineTemplateCatalogSyncGate = $true
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
  frontend = "electron_compatibility"
  channel = $Channel
  generated_at = (Get-Date).ToString("s")
  frontend_verification = Get-FrontendVerificationSummary -Root $root
  architecture_scorecard = $architectureScorecardSummary
  sidecar_regression = $sidecarRegressionSummary
  sidecar_python_rust_consistency = $sidecarConsistencySummary
  gates = [ordered]@{
    architecture_scorecard = [ordered]@{
      status = $architectureScorecardGateStatus
      checked_at = $architectureScorecardGateCheckedAt
      script = "ops/logs/architecture/architecture_scorecard_release_ready_latest.json"
      error = $architectureScorecardGateError
    }
    sidecar_regression = [ordered]@{
      status = $sidecarRegressionGateStatus
      checked_at = $sidecarRegressionGateCheckedAt
      script = "ops/logs/regression/sidecar_regression_quality_report.json"
      error = $sidecarRegressionGateError
    }
    sidecar_python_rust_consistency = [ordered]@{
      status = $sidecarConsistencyGateStatus
      checked_at = $sidecarConsistencyGateCheckedAt
      script = "ops/logs/regression/sidecar_python_rust_consistency_report.json"
      error = $sidecarConsistencyGateError
    }
    openapi_sdk_sync = [ordered]@{
      status = $openApiSdkSyncGateStatus
      checked_at = $openApiSdkSyncGateCheckedAt
      script = "ops/scripts/check_openapi_sdk_sync.ps1"
      error = $openApiSdkSyncGateError
    }
    workflow_contract_sync = [ordered]@{
      status = $workflowContractSyncGateStatus
      checked_at = $workflowContractSyncGateCheckedAt
      script = "ops/scripts/check_workflow_contract_sync.ps1"
      error = $workflowContractSyncGateError
    }
    governance_capability_export = [ordered]@{
      status = $governanceCapabilityExportStatus
      checked_at = $governanceCapabilityExportCheckedAt
      script = "ops/scripts/export_governance_capabilities.ps1"
      error = $governanceCapabilityExportError
    }
    governance_control_plane_boundary = [ordered]@{
      status = $governanceControlPlaneBoundaryGateStatus
      checked_at = $governanceControlPlaneBoundaryGateCheckedAt
      script = "ops/scripts/check_governance_control_plane_boundary.ps1"
      error = $governanceControlPlaneBoundaryGateError
    }
    operator_catalog_sync = [ordered]@{
      status = $operatorCatalogSyncGateStatus
      checked_at = $operatorCatalogSyncGateCheckedAt
      script = "ops/scripts/check_operator_catalog_sync.ps1"
      error = $operatorCatalogSyncGateError
    }
    fallback_governance = [ordered]@{
      status = $fallbackGovernanceGateStatus
      checked_at = $fallbackGovernanceGateCheckedAt
      script = "ops/scripts/check_fallback_governance.ps1"
      error = $fallbackGovernanceGateError
    }
    governance_store_schema_versions = [ordered]@{
      status = $governanceStoreSchemaVersionsGateStatus
      checked_at = $governanceStoreSchemaVersionsGateCheckedAt
      script = "ops/scripts/check_governance_store_schema_versions.ps1"
      error = $governanceStoreSchemaVersionsGateError
    }
    local_workflow_store_schema_versions = [ordered]@{
      status = $localWorkflowStoreSchemaVersionsGateStatus
      checked_at = $localWorkflowStoreSchemaVersionsGateCheckedAt
      script = "ops/scripts/check_local_workflow_store_schema_versions.ps1"
      error = $localWorkflowStoreSchemaVersionsGateError
    }
    template_pack_contract_sync = [ordered]@{
      status = $templatePackContractSyncGateStatus
      checked_at = $templatePackContractSyncGateCheckedAt
      script = "ops/scripts/check_template_pack_contract_sync.ps1"
      error = $templatePackContractSyncGateError
    }
    local_template_storage_contract_sync = [ordered]@{
      status = $localTemplateStorageContractSyncGateStatus
      checked_at = $localTemplateStorageContractSyncGateCheckedAt
      script = "ops/scripts/check_local_template_storage_contract_sync.ps1"
      error = $localTemplateStorageContractSyncGateError
    }
    offline_template_catalog_sync = [ordered]@{
      status = $offlineTemplateCatalogSyncGateStatus
      checked_at = $offlineTemplateCatalogSyncGateCheckedAt
      script = "ops/scripts/check_offline_template_catalog_sync.ps1"
      error = $offlineTemplateCatalogSyncGateError
    }
  }
}
($audit | ConvertTo-Json -Depth 6) | Set-Content $auditPath -Encoding UTF8
Ok "release gate audit written: $auditPath"

Ok "release ready: release/offline_bundle_${Version}_installer and _portable"
