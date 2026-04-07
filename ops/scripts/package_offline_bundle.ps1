param(
  [string]$OutDir = "",
  [string]$Version = "",
  [ValidateSet("installer", "portable")]
  [string]$PackageType = "installer",
  [switch]$IncludeBundledTools,
  [switch]$CollectBundledTools,
  [switch]$CleanOldReleases,
  [switch]$SkipOpenApiSdkSyncGate,
  [switch]$SkipWorkflowContractSyncGate,
  [switch]$SkipGovernanceControlPlaneBoundaryGate,
  [switch]$SkipOperatorCatalogSyncGate,
  [switch]$SkipCleaningRustV2RolloutGate,
  [switch]$SkipGovernanceStoreSchemaVersionsGate,
  [switch]$SkipLocalWorkflowStoreSchemaVersionsGate,
  [switch]$SkipTemplatePackContractSyncGate,
  [switch]$SkipLocalTemplateStorageContractSyncGate,
  [switch]$SkipOfflineTemplateCatalogSyncGate,
  [bool]$RequireChineseOcr = $true,
  [string]$ReleaseChannel = "stable"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
. (Join-Path $PSScriptRoot "governance_capability_export_support.ps1")
function Get-CleaningRustV2RolloutSettings() {
  return [ordered]@{
    mode = "default"
    verify_on_default = $true
  }
}
function Get-CleaningShadowAcceptanceTargets([string]$Root, [string]$GateScope) {
  return @(
    [ordered]@{
      name = "desktop_real_sample"
      evidence_path = Join-Path $Root "ops\logs\acceptance\desktop_real_sample\cleaning_shadow_rollout.json"
      gate_out_dir = Join-Path $Root ("ops\logs\cleaning_rollout\{0}\desktop_real_sample" -f $GateScope)
    },
    [ordered]@{
      name = "desktop_finance_template"
      evidence_path = Join-Path $Root "ops\logs\acceptance\desktop_finance_template\cleaning_shadow_rollout.json"
      gate_out_dir = Join-Path $Root ("ops\logs\cleaning_rollout\{0}\desktop_finance_template" -f $GateScope)
    }
  )
}
function Get-CleaningShadowAcceptanceEvidence([string]$Path) {
  $summary = [ordered]@{
    evidence_path = $Path
    exists = $false
    acceptance = ""
    ok = $false
    generated_at = ""
    requested_rust_v2_mode = ""
    effective_rust_v2_mode = ""
    verify_on_default = $false
    run_mode_audit_path = ""
    sample_result_path = ""
    acceptance_report_path = ""
    execution_mode = ""
    rust_v2_used = $false
    shadow_compare = [ordered]@{
      status = ""
      matched = $false
      mismatch_count = 0
      skipped_reason = ""
      compare_fields = @()
    }
  }
  if (-not (Test-Path $Path)) {
    return $summary
  }
  try {
    $raw = Get-Content -Raw -Encoding UTF8 $Path | ConvertFrom-Json
    $summary.exists = $true
    $summary.acceptance = [string]($raw.acceptance)
    $summary.ok = [bool]$raw.ok
    $summary.generated_at = [string]($raw.generated_at)
    $summary.requested_rust_v2_mode = [string]($raw.requested_rust_v2_mode)
    $summary.effective_rust_v2_mode = [string]($raw.effective_rust_v2_mode)
    $summary.verify_on_default = [bool]$raw.verify_on_default
    $summary.run_mode_audit_path = [string]($raw.run_mode_audit_path)
    $summary.sample_result_path = [string]($raw.sample_result_path)
    $summary.acceptance_report_path = [string]($raw.acceptance_report_path)
    $summary.execution_mode = [string]($raw.execution.execution_mode)
    $summary.rust_v2_used = [bool]$raw.quality.rust_v2_used
    $summary.shadow_compare = if ($raw.shadow_compare) { $raw.shadow_compare } else { $summary.shadow_compare }
  } catch {
    $summary.exists = $true
    $summary.acceptance = "unreadable"
  }
  return $summary
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
    throw ("package blocked by {0}: missing report {1}. Run the sidecar regression verification first." -f $Label, [string]$Summary.evidence_path)
  }
  if (-not $Summary.ok) {
    throw ("package blocked by {0}: report not ok ({1})" -f $Label, [string]$Summary.evidence_path)
  }
  if ($RequireNoSkipped -and @($Summary.skipped).Count -gt 0) {
    throw ("package blocked by {0}: skipped entries present ({1})" -f $Label, (@($Summary.skipped) -join ", "))
  }
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$sidecarRegressionSummary = Get-SidecarReportSummary -Root $root -Name "sidecar_regression_quality_report"
$sidecarConsistencySummary = Get-SidecarReportSummary -Root $root -Name "sidecar_python_rust_consistency_report"
$openApiSdkSyncGate = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$workflowContractSyncGate = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceCapabilityExportScript = Join-Path $PSScriptRoot "export_governance_capabilities.ps1"
$governanceControlPlaneBoundaryGate = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$operatorCatalogSyncGate = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$cleaningRustV2RolloutGate = Join-Path $PSScriptRoot "check_cleaning_rust_v2_rollout.ps1"
$cleaningRustV2RolloutSummaryScript = Join-Path $PSScriptRoot "summarize_cleaning_rust_v2_rollout.ps1"
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
$cleaningRustV2RolloutGateStatus = "skipped"
$cleaningRustV2RolloutGateCheckedAt = ""
$cleaningRustV2RolloutGateError = ""
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
$sidecarRegressionGateStatus = "skipped"
$sidecarRegressionGateCheckedAt = ""
$sidecarRegressionGateError = ""
$sidecarConsistencyGateStatus = "skipped"
$sidecarConsistencyGateCheckedAt = ""
$sidecarConsistencyGateError = ""
$cleaningRustV2RolloutSettings = Get-CleaningRustV2RolloutSettings
$cleaningShadowAcceptanceTargets = Get-CleaningShadowAcceptanceTargets -Root $root -GateScope "package_offline_bundle"
$cleaningShadowAcceptanceEvidence = [ordered]@{}
$cleaningRustV2RunModeAuditPaths = @()
$cleaningRustV2ShadowCompareSummary = [ordered]@{}
$cleaningRustV2SummaryPath = Join-Path $root "ops\logs\cleaning_rollout\summary\cleaning_rust_v2_rollout_summary_latest.json"
$cleaningRustV2Summary = $null
$distDir = Join-Path $root "apps\dify-desktop\dist"
$exePattern = if ($PackageType -eq "installer") { "AIWF Dify Desktop Setup *.exe" } else { "AIWF Dify Desktop *.exe" }
$exe = Get-ChildItem $distDir -File -Filter $exePattern |
  Where-Object {
    $_.Name -notlike "*.blockmap" -and
    (($PackageType -eq "installer" -and $_.Name -like "*Setup*") -or ($PackageType -eq "portable" -and $_.Name -notlike "*Setup*"))
  } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
if (-not $exe) {
  throw "$PackageType exe not found under: $distDir"
}

if (-not $Version) {
  if ($exe.Name -match 'Desktop\s+(.+)\.exe$') {
    $Version = $Matches[1].Trim()
  } else {
    $Version = "latest"
  }
}

if (-not $OutDir) {
  $OutDir = Join-Path $root ("release\offline_bundle_{0}_{1}" -f $Version, $PackageType)
}

Info "running sidecar regression package gate"
try {
  Assert-SidecarReportReady $sidecarRegressionSummary "sidecar regression package gate"
  $sidecarRegressionGateStatus = "passed"
  $sidecarRegressionGateCheckedAt = (Get-Date).ToString("s")
  Ok "sidecar regression package gate passed"
} catch {
  $sidecarRegressionGateStatus = "failed"
  $sidecarRegressionGateCheckedAt = (Get-Date).ToString("s")
  $sidecarRegressionGateError = [string]$_.Exception.Message
  throw
}

Info "running sidecar python/rust consistency package gate"
try {
  Assert-SidecarReportReady $sidecarConsistencySummary "sidecar python/rust consistency package gate" -RequireNoSkipped
  $sidecarConsistencyGateStatus = "passed"
  $sidecarConsistencyGateCheckedAt = (Get-Date).ToString("s")
  Ok "sidecar python/rust consistency package gate passed"
} catch {
  $sidecarConsistencyGateStatus = "failed"
  $sidecarConsistencyGateCheckedAt = (Get-Date).ToString("s")
  $sidecarConsistencyGateError = [string]$_.Exception.Message
  throw
}

if (-not $SkipOpenApiSdkSyncGate) {
  if (-not (Test-Path $openApiSdkSyncGate)) { throw "openapi/sdk sync gate script missing: $openApiSdkSyncGate" }
  Info "running openapi/sdk sync package gate"
  powershell -ExecutionPolicy Bypass -File $openApiSdkSyncGate
  $openApiSdkSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $openApiSdkSyncGateStatus = "failed"
    $openApiSdkSyncGateError = "check_openapi_sdk_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by openapi/sdk sync gate"
  }
  $openApiSdkSyncGateStatus = "passed"
  Ok "openapi/sdk sync package gate passed"
} else {
  $openApiSdkSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip openapi/sdk sync package gate" -ForegroundColor Yellow
}

if (-not $SkipWorkflowContractSyncGate) {
  if (-not (Test-Path $workflowContractSyncGate)) { throw "workflow contract sync gate script missing: $workflowContractSyncGate" }
  Info "running workflow contract sync package gate"
  powershell -ExecutionPolicy Bypass -File $workflowContractSyncGate
  $workflowContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $workflowContractSyncGateStatus = "failed"
    $workflowContractSyncGateError = "check_workflow_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by workflow contract sync gate"
  }
  $workflowContractSyncGateStatus = "passed"
  Ok "workflow contract sync package gate passed"
} else {
  $workflowContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip workflow contract sync package gate" -ForegroundColor Yellow
}

$governanceCapabilityExportResult = Invoke-GovernanceCapabilityExportStep -ScriptPath $governanceCapabilityExportScript -FailureScope "package"
$governanceCapabilityExportCheckedAt = [string]$governanceCapabilityExportResult.checked_at
if (-not $governanceCapabilityExportResult.ok) {
  $governanceCapabilityExportStatus = "failed"
  $governanceCapabilityExportError = [string]$governanceCapabilityExportResult.error
  throw [string]$governanceCapabilityExportResult.failure_message
}
$governanceCapabilityExportStatus = "passed"

if (-not $SkipGovernanceControlPlaneBoundaryGate) {
  if (-not (Test-Path $governanceControlPlaneBoundaryGate)) { throw "governance control plane boundary gate script missing: $governanceControlPlaneBoundaryGate" }
  Info "running governance control plane boundary package gate"
  powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryGate
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $governanceControlPlaneBoundaryGateStatus = "failed"
    $governanceControlPlaneBoundaryGateError = "check_governance_control_plane_boundary.ps1 exit code $LASTEXITCODE"
    throw "package blocked by governance control plane boundary gate"
  }
  $governanceControlPlaneBoundaryGateStatus = "passed"
  Ok "governance control plane boundary package gate passed"
} else {
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance control plane boundary package gate" -ForegroundColor Yellow
}

if (-not $SkipOperatorCatalogSyncGate) {
  if (-not (Test-Path $operatorCatalogSyncGate)) { throw "operator catalog sync gate script missing: $operatorCatalogSyncGate" }
  Info "running operator catalog sync package gate"
  powershell -ExecutionPolicy Bypass -File $operatorCatalogSyncGate
  $operatorCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $operatorCatalogSyncGateStatus = "failed"
    $operatorCatalogSyncGateError = "check_operator_catalog_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by operator catalog sync gate"
  }
  $operatorCatalogSyncGateStatus = "passed"
  Ok "operator catalog sync package gate passed"
} else {
  $operatorCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip operator catalog sync package gate" -ForegroundColor Yellow
}

if (-not $SkipCleaningRustV2RolloutGate) {
  if (-not (Test-Path $cleaningRustV2RolloutGate)) { throw "cleaning rust v2 rollout gate script missing: $cleaningRustV2RolloutGate" }
  foreach ($target in $cleaningShadowAcceptanceTargets) {
    Info ("running cleaning rust v2 rollout package gate ({0})" -f [string]$target.name)
    powershell -ExecutionPolicy Bypass -File $cleaningRustV2RolloutGate `
      -OutDir $target.gate_out_dir `
      -EvidencePath $target.evidence_path `
      -ConsistencyReportPath $sidecarConsistencySummary.evidence_path `
      -RequestedMode $cleaningRustV2RolloutSettings.mode `
      -VerifyOnDefault:$cleaningRustV2RolloutSettings.verify_on_default `
      -RequireRealEvidence `
      -RequireNoSkipped
    $cleaningRustV2RolloutGateCheckedAt = (Get-Date).ToString("s")
    if ($LASTEXITCODE -ne 0) {
      $cleaningRustV2RolloutGateStatus = "failed"
      $cleaningRustV2RolloutGateError = "check_cleaning_rust_v2_rollout.ps1 exit code $LASTEXITCODE ($([string]$target.name))"
      throw ("package blocked by cleaning rust v2 rollout gate ({0})" -f [string]$target.name)
    }
    $evidence = Get-CleaningShadowAcceptanceEvidence -Path $target.evidence_path
    $cleaningShadowAcceptanceEvidence[$target.name] = $evidence
    if (-not [string]::IsNullOrWhiteSpace([string]$evidence.run_mode_audit_path)) {
      $cleaningRustV2RunModeAuditPaths += [string]$evidence.run_mode_audit_path
    }
    $cleaningRustV2ShadowCompareSummary[$target.name] = $evidence.shadow_compare
  }
  $cleaningRustV2RolloutGateStatus = "passed"
  if (Test-Path $cleaningRustV2RolloutSummaryScript) {
    powershell -ExecutionPolicy Bypass -File $cleaningRustV2RolloutSummaryScript -RepoRoot $root | Out-Null
    if (Test-Path $cleaningRustV2SummaryPath) {
      $cleaningRustV2Summary = Get-Content -Raw -Encoding UTF8 $cleaningRustV2SummaryPath | ConvertFrom-Json
    }
  }
  Ok "cleaning rust v2 rollout package gate passed"
} else {
  $cleaningRustV2RolloutGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip cleaning rust v2 rollout package gate" -ForegroundColor Yellow
}

if (-not $SkipGovernanceStoreSchemaVersionsGate) {
  if (-not (Test-Path $governanceStoreSchemaVersionsGate)) { throw "governance store schema version gate script missing: $governanceStoreSchemaVersionsGate" }
  Info "running governance store schema version package gate"
  powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionsGate
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $governanceStoreSchemaVersionsGateStatus = "failed"
    $governanceStoreSchemaVersionsGateError = "check_governance_store_schema_versions.ps1 exit code $LASTEXITCODE"
    throw "package blocked by governance store schema version gate"
  }
  $governanceStoreSchemaVersionsGateStatus = "passed"
  Ok "governance store schema version package gate passed"
} else {
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance store schema version package gate" -ForegroundColor Yellow
}

if (-not $SkipLocalWorkflowStoreSchemaVersionsGate) {
  if (-not (Test-Path $localWorkflowStoreSchemaVersionsGate)) { throw "local workflow store schema version gate script missing: $localWorkflowStoreSchemaVersionsGate" }
  Info "running local workflow store schema version package gate"
  powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionsGate
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $localWorkflowStoreSchemaVersionsGateStatus = "failed"
    $localWorkflowStoreSchemaVersionsGateError = "check_local_workflow_store_schema_versions.ps1 exit code $LASTEXITCODE"
    throw "package blocked by local workflow store schema version gate"
  }
  $localWorkflowStoreSchemaVersionsGateStatus = "passed"
  Ok "local workflow store schema version package gate passed"
} else {
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local workflow store schema version package gate" -ForegroundColor Yellow
}

if (-not $SkipTemplatePackContractSyncGate) {
  if (-not (Test-Path $templatePackContractSyncGate)) { throw "template pack contract sync gate script missing: $templatePackContractSyncGate" }
  Info "running template pack contract package gate"
  powershell -ExecutionPolicy Bypass -File $templatePackContractSyncGate
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $templatePackContractSyncGateStatus = "failed"
    $templatePackContractSyncGateError = "check_template_pack_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by template pack contract gate"
  }
  $templatePackContractSyncGateStatus = "passed"
  Ok "template pack contract package gate passed"
} else {
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip template pack contract package gate" -ForegroundColor Yellow
}

if (-not $SkipLocalTemplateStorageContractSyncGate) {
  if (-not (Test-Path $localTemplateStorageContractSyncGate)) { throw "local template storage contract gate script missing: $localTemplateStorageContractSyncGate" }
  Info "running local template storage contract package gate"
  powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncGate
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $localTemplateStorageContractSyncGateStatus = "failed"
    $localTemplateStorageContractSyncGateError = "check_local_template_storage_contract_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by local template storage contract gate"
  }
  $localTemplateStorageContractSyncGateStatus = "passed"
  Ok "local template storage contract package gate passed"
} else {
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local template storage contract package gate" -ForegroundColor Yellow
}

if (-not $SkipOfflineTemplateCatalogSyncGate) {
  if (-not (Test-Path $offlineTemplateCatalogSyncGate)) { throw "offline template catalog sync gate script missing: $offlineTemplateCatalogSyncGate" }
  Info "running offline template catalog package gate"
  powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncGate
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    $offlineTemplateCatalogSyncGateStatus = "failed"
    $offlineTemplateCatalogSyncGateError = "check_offline_template_catalog_sync.ps1 exit code $LASTEXITCODE"
    throw "package blocked by offline template catalog gate"
  }
  $offlineTemplateCatalogSyncGateStatus = "passed"
  Ok "offline template catalog package gate passed"
} else {
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip offline template catalog package gate" -ForegroundColor Yellow
}

$bundleRoot = Join-Path $OutDir "AIWF_Offline_Bundle"
$docsOut = Join-Path $bundleRoot "docs"
$contractsOut = Join-Path $bundleRoot "contracts\desktop"
$workflowContractsOut = Join-Path $bundleRoot "contracts\workflow"
$rustContractsOut = Join-Path $bundleRoot "contracts\rust"
$glueContractsOut = Join-Path $bundleRoot "contracts\glue"
$governanceContractsOut = Join-Path $bundleRoot "contracts\governance"
if ($CleanOldReleases) {
  $releaseRoot = Join-Path $root "release"
  if (Test-Path $releaseRoot) {
    Get-ChildItem $releaseRoot -Directory -Filter "offline_bundle_*" |
      Where-Object { $_.FullName -ne $OutDir } |
      ForEach-Object { Remove-Item $_.FullName -Recurse -Force }
  }
}

Info "preparing output dir: $bundleRoot"
if (Test-Path $bundleRoot) {
  Remove-Item $bundleRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $docsOut -Force | Out-Null
New-Item -ItemType Directory -Path $contractsOut -Force | Out-Null
New-Item -ItemType Directory -Path $workflowContractsOut -Force | Out-Null
New-Item -ItemType Directory -Path $rustContractsOut -Force | Out-Null
New-Item -ItemType Directory -Path $glueContractsOut -Force | Out-Null
New-Item -ItemType Directory -Path $governanceContractsOut -Force | Out-Null

Info "copy desktop exe"
Copy-Item $exe.FullName (Join-Path $bundleRoot $exe.Name)
$blockmap = "$($exe.FullName).blockmap"
if (Test-Path $blockmap) {
  Copy-Item $blockmap (Join-Path $bundleRoot (Split-Path $blockmap -Leaf))
}

$docList = @(
  "docs\quickstart_desktop_offline.md",
  "docs\dify_desktop_app.md",
  "docs\offline_delivery_minimal.md",
  "docs\finance_template_v1.md",
  "docs\regression_quality.md"
)
foreach ($d in $docList) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $docsOut (Split-Path $src -Leaf))
  }
}

$desktopContractSchemas = @(
  "contracts\desktop\template_pack_artifact.schema.json",
  "contracts\desktop\local_template_storage.schema.json",
  "contracts\desktop\office_theme_catalog.schema.json",
  "contracts\desktop\office_layout_catalog.schema.json",
  "contracts\desktop\cleaning_template_registry.schema.json",
  "contracts\desktop\offline_template_catalog_pack_manifest.schema.json"
)
foreach ($schemaRel in $desktopContractSchemas) {
  $src = Join-Path $root $schemaRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $contractsOut (Split-Path $src -Leaf))
  }
}

$workflowContractFiles = @(
  "contracts\workflow\workflow.schema.json",
  "contracts\workflow\render_contract.schema.json",
  "contracts\workflow\minimal_workflow.v1.json"
)
foreach ($contractRel in $workflowContractFiles) {
  $src = Join-Path $root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $workflowContractsOut (Split-Path $src -Leaf))
  }
}

$rustContractFiles = @(
  "contracts\rust\operators_manifest.v1.json",
  "contracts\rust\operators_manifest.schema.json",
  "contracts\rust\operators_extension_v1.schema.json",
  "contracts\rust\transform_rows_v2.schema.json"
)
foreach ($contractRel in $rustContractFiles) {
  $src = Join-Path $root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $rustContractsOut (Split-Path $src -Leaf))
  }
}

$glueContractFiles = @(
  "contracts\glue\ingest_extract.schema.json"
)
foreach ($contractRel in $glueContractFiles) {
  $src = Join-Path $root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $glueContractsOut (Split-Path $src -Leaf))
  }
}

$governanceContractFiles = @(
  "contracts\governance\governance_capabilities.v1.json"
)
foreach ($contractRel in $governanceContractFiles) {
  $src = Join-Path $root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $governanceContractsOut (Split-Path $src -Leaf))
  }
}

if ($IncludeBundledTools) {
  if ($CollectBundledTools) {
    $collector = Join-Path $root "ops\scripts\collect_offline_tools.ps1"
    if (Test-Path $collector) {
      Info "collect bundled tools from local machine"
      powershell -ExecutionPolicy Bypass -File $collector -DesktopDir (Join-Path $root "apps\dify-desktop")
    }
  }
  $toolSrc = Join-Path $root "apps\dify-desktop\tools"
  $toolDst = Join-Path $bundleRoot "tools"
  if (Test-Path $toolSrc) {
    Info "copy bundled tools"
    Copy-Item $toolSrc $toolDst -Recurse -Force
    if ($RequireChineseOcr) {
      $chi = Join-Path $toolDst "tesseract\tessdata\chi_sim.traineddata"
      if (-not (Test-Path $chi)) {
        throw "chi_sim.traineddata missing in bundled tools: $chi"
      }
    }
  } else {
    Info "bundled tools source not found, skip: $toolSrc"
  }
}

$lines = @(
  "# AIWF 离线交付包",
  "",
  "## 内容",
  "- 可执行文件: $($exe.Name)",
  "- 包类型: $PackageType",
  "- 发布通道: $ReleaseChannel",
  "- 文档目录: docs/",
  "- 契约目录: contracts/desktop/",
  "- 工作流契约目录: contracts/workflow/",
  "- Rust 契约目录: contracts/rust/",
  "- Glue 契约目录: contracts/glue/",
  "- Governance 契约目录: contracts/governance/",
  "- 版本目录: $(Split-Path $OutDir -Leaf)",
  "- Sidecar regression report: $([string]$sidecarRegressionSummary.evidence_path)",
  "- Sidecar consistency report: $([string]$sidecarConsistencySummary.evidence_path)",
  "- 打包前会校验 sidecar regression 报告 ok=true，且 Python/Rust consistency 报告不允许存在 skipped。",
  "",
  "## 安装与使用",
  "1. 双击运行 exe。",
  "2. 启动桌面应用。",
  "3. 保持在 离线本地模式。",
  "4. 将生肉文件拖入任务队列后点击 开始生成。",
  "5. 若包含 tools/，应用会优先使用内置 OCR 依赖（tesseract/pdftoppm）。",
  "6. 若要启用 image/xlsx 增强清洗，请确保本地 glue-python sidecar 以 -RequireEnhancedIngest 方式启动并满足 docling/paddleocr/python-calamine/pandera 依赖。",
  "",
  "## 默认输出目录",
  "- 若存在 E:\\Desktop_Real，则默认输出到 E:\\Desktop_Real\\AIWF\\<job_id>\\artifacts",
  "- 否则默认输出到 桌面\\AIWF_Builds\\<job_id>\\artifacts",
  "- 若用户在应用内修改 输出目录，则以用户配置为准"
)
$lines | Set-Content (Join-Path $bundleRoot "README.txt") -Encoding UTF8

$sha = Get-FileHash (Join-Path $bundleRoot $exe.Name) -Algorithm SHA256
("{0}  {1}" -f $sha.Hash, $exe.Name) | Set-Content (Join-Path $bundleRoot "SHA256SUMS.txt") -Encoding ASCII

$manifest = [ordered]@{
  product = "AIWF Dify Desktop"
  version = $Version
  package_type = $PackageType
  release_channel = $ReleaseChannel
  exe = $exe.Name
  generated_at = (Get-Date).ToString("s")
  docs = @((Get-ChildItem $docsOut -File | ForEach-Object { $_.Name }))
  contract_schemas = @((Get-ChildItem $contractsOut -File | ForEach-Object { ("contracts/desktop/" + $_.Name) }))
  workflow_contracts = @((Get-ChildItem $workflowContractsOut -File | ForEach-Object { ("contracts/workflow/" + $_.Name) }))
  rust_contracts = @((Get-ChildItem $rustContractsOut -File | ForEach-Object { ("contracts/rust/" + $_.Name) }))
  glue_contracts = @((Get-ChildItem $glueContractsOut -File | ForEach-Object { ("contracts/glue/" + $_.Name) }))
  governance_contracts = @((Get-ChildItem $governanceContractsOut -File | ForEach-Object { ("contracts/governance/" + $_.Name) }))
  regression_reports = [ordered]@{
    sidecar_regression = $sidecarRegressionSummary
    sidecar_python_rust_consistency = $sidecarConsistencySummary
  }
  cleaning_rust_v2_rollout = [ordered]@{
    mode = [string]$cleaningRustV2RolloutSettings.mode
    verify_on_default = [bool]$cleaningRustV2RolloutSettings.verify_on_default
    gate_status = $cleaningRustV2RolloutGateStatus
    acceptance_evidence = $cleaningShadowAcceptanceEvidence
    run_mode_audit_paths = @($cleaningRustV2RunModeAuditPaths | Select-Object -Unique)
    sidecar_consistency_report_path = [string]$sidecarConsistencySummary.evidence_path
    latest_shadow_compare_summary = $cleaningRustV2ShadowCompareSummary
    summary_path = $cleaningRustV2SummaryPath
    summary = $cleaningRustV2Summary
  }
  gates = [ordered]@{
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
    cleaning_rust_v2_rollout = [ordered]@{
      status = $cleaningRustV2RolloutGateStatus
      checked_at = $cleaningRustV2RolloutGateCheckedAt
      script = "ops/scripts/check_cleaning_rust_v2_rollout.ps1"
      error = $cleaningRustV2RolloutGateError
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
($manifest | ConvertTo-Json -Depth 5) | Set-Content (Join-Path $bundleRoot "manifest.json") -Encoding UTF8

$notes = @(
  "# Release Notes",
  "",
  "- Version: $Version",
  "- Channel: $ReleaseChannel",
  "- PackageType: $PackageType",
  "- BuiltAt: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))",
  "",
  "## Included",
  "- $(($manifest.docs -join ', '))",
  "- SHA256SUMS.txt",
  "- manifest.json"
)
$notes | Set-Content (Join-Path $bundleRoot "RELEASE_NOTES.md") -Encoding UTF8

Ok "offline bundle ready: $bundleRoot"
