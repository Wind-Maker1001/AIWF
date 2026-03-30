param(
  [Parameter(Mandatory = $true)][string]$Version,
  [ValidateSet("WinUI", "Electron")]
  [string]$Frontend = "WinUI",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [string]$Channel = "stable",
  [ValidateSet("PersonalSideload", "ManagedTrusted")]
  [string]$ReleaseAudience = "PersonalSideload",
  [switch]$CreateZip,
  [switch]$IncludeMsix,
  [ValidateSet("PersonalSideloadCert", "PreviewSelfSigned", "ProvidedPfx", "StoreThumbprint")]
  [string]$MsixSigningMode = "PersonalSideloadCert",
  [string]$MsixPfxPath = "",
  [string]$MsixPfxPassword = "",
  [string]$MsixCertificatePath = "",
  [string]$MsixSigningThumbprint = "",
  [string]$MsixTimestampUrl = "",
  [switch]$GenerateAppInstaller,
  [string]$MsixAppInstallerUriBase = "",
  [switch]$AllowPreviewMsixOnStable,
  [int]$PersonalSideloadCertWarnWhenExpiresInDays = 30,
  [int]$PersonalSideloadCertFailWhenExpiresInDays = 14,
  [switch]$AllowExpiringPersonalSideloadCert,
  [string]$EnvFile = "",
  [switch]$SkipSqlConnectivityGate,
  [switch]$SkipFrontendConvergenceGate,
  [switch]$SkipWorkflowContractSyncGate,
  [switch]$SkipGovernanceControlPlaneBoundaryGate,
  [switch]$SkipOperatorCatalogSyncGate,
  [switch]$SkipFallbackGovernanceGate,
  [switch]$SkipGovernanceStoreSchemaVersionsGate,
  [switch]$SkipLocalWorkflowStoreSchemaVersionsGate,
  [switch]$SkipTemplatePackContractSyncGate,
  [switch]$SkipLocalTemplateStorageContractSyncGate,
  [switch]$SkipOfflineTemplateCatalogSyncGate,
  [switch]$SkipNativeWinuiSmokeGate,
  [switch]$KeepGeneratedMsixCertificate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Extract-LastPathLine([object]$Output) {
  $text = [string]::Join("`n", @($Output))
  $matches = $text -split "`r?`n" | Where-Object { $_ -match '^[A-Za-z]:\\' }
  $last = $matches | Select-Object -Last 1
  if ($null -eq $last) { return "" }
  return [string]$last
}
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

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

if ($Frontend -eq "Electron") {
  Warn "Electron release path is compatibility-only. WinUI is the primary frontend."
  $legacy = Join-Path $PSScriptRoot "release_electron_compatibility.ps1"
  & $legacy -Version $Version -Channel $Channel -EnvFile $EnvFile -SkipSqlConnectivityGate:$SkipSqlConnectivityGate -SkipWorkflowContractSyncGate:$SkipWorkflowContractSyncGate -SkipGovernanceControlPlaneBoundaryGate:$SkipGovernanceControlPlaneBoundaryGate -SkipOperatorCatalogSyncGate:$SkipOperatorCatalogSyncGate -SkipFallbackGovernanceGate:$SkipFallbackGovernanceGate -SkipGovernanceStoreSchemaVersionsGate:$SkipGovernanceStoreSchemaVersionsGate -SkipLocalWorkflowStoreSchemaVersionsGate:$SkipLocalWorkflowStoreSchemaVersionsGate -SkipTemplatePackContractSyncGate:$SkipTemplatePackContractSyncGate -SkipLocalTemplateStorageContractSyncGate:$SkipLocalTemplateStorageContractSyncGate -SkipOfflineTemplateCatalogSyncGate:$SkipOfflineTemplateCatalogSyncGate
  exit $LASTEXITCODE
}

if ($IncludeMsix -and $Channel -eq "stable" -and $ReleaseAudience -eq "ManagedTrusted" -and @("PreviewSelfSigned", "PersonalSideloadCert") -contains $MsixSigningMode -and (-not $AllowPreviewMsixOnStable)) {
  throw "stable WinUI releases for ManagedTrusted distribution cannot ship preview or personal sideload signing. Provide -MsixSigningMode ProvidedPfx or StoreThumbprint, or explicitly override with -AllowPreviewMsixOnStable for local dry runs."
}
if ($IncludeMsix -and $Channel -eq "stable" -and $ReleaseAudience -eq "ManagedTrusted" -and $MsixSigningMode -ne "PreviewSelfSigned" -and (-not $GenerateAppInstaller)) {
  throw "stable WinUI releases for ManagedTrusted distribution must also generate an appinstaller. Pass -GenerateAppInstaller and -MsixAppInstallerUriBase."
}
if ($IncludeMsix -and $MsixSigningMode -eq "ProvidedPfx" -and [string]::IsNullOrWhiteSpace($MsixPfxPath)) {
  throw "MsixPfxPath is required when -IncludeMsix and -MsixSigningMode ProvidedPfx"
}
if ($IncludeMsix -and $MsixSigningMode -eq "StoreThumbprint" -and [string]::IsNullOrWhiteSpace($MsixSigningThumbprint)) {
  throw "MsixSigningThumbprint is required when -IncludeMsix and -MsixSigningMode StoreThumbprint"
}
if ($GenerateAppInstaller -and [string]::IsNullOrWhiteSpace($MsixAppInstallerUriBase)) {
  throw "MsixAppInstallerUriBase is required when -GenerateAppInstaller"
}

$frontendGate = Join-Path $PSScriptRoot "check_frontend_convergence.ps1"
$workflowGate = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceCapabilityExportScript = Join-Path $PSScriptRoot "export_governance_capabilities.ps1"
$governanceControlPlaneBoundaryGate = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$operatorGate = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$fallbackGate = Join-Path $PSScriptRoot "check_fallback_governance.ps1"
$governanceStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_governance_store_schema_versions.ps1"
$localWorkflowStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_local_workflow_store_schema_versions.ps1"
$templatePackContractSyncGate = Join-Path $PSScriptRoot "check_template_pack_contract_sync.ps1"
$localTemplateStorageContractSyncGate = Join-Path $PSScriptRoot "check_local_template_storage_contract_sync.ps1"
$offlineTemplateCatalogSyncGate = Join-Path $PSScriptRoot "check_offline_template_catalog_sync.ps1"
$sqlGate = Join-Path $PSScriptRoot "check_sql_connectivity.ps1"
$ensurePersonalSideloadCert = Join-Path $PSScriptRoot "ensure_personal_sideload_certificate.ps1"
$checkPersonalSideloadCert = Join-Path $PSScriptRoot "check_personal_sideload_certificate.ps1"
$packageScript = Join-Path $PSScriptRoot "package_native_winui_bundle.ps1"
$msixScript = Join-Path $PSScriptRoot "package_native_winui_msix.ps1"
$outRoot = Join-Path $root "release"
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null

if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}

$frontendGateStatus = "skipped"
$workflowGateStatus = "skipped"
$governanceCapabilityExportStatus = "skipped"
$governanceControlPlaneBoundaryGateStatus = "skipped"
$operatorGateStatus = "skipped"
$fallbackGateStatus = "skipped"
$governanceStoreSchemaVersionsGateStatus = "skipped"
$localWorkflowStoreSchemaVersionsGateStatus = "skipped"
$templatePackContractSyncGateStatus = "skipped"
$localTemplateStorageContractSyncGateStatus = "skipped"
$offlineTemplateCatalogSyncGateStatus = "skipped"
$sqlGateStatus = "skipped"
$personalSideloadCertStatus = "skipped"
$architectureScorecardGateStatus = "skipped"
$sidecarRegressionGateStatus = "skipped"
$sidecarConsistencyGateStatus = "skipped"
$bundleRoot = ""
$msixPath = ""
$architectureScorecardSummary = Get-ArchitectureScorecardSummary -Root $root
$sidecarRegressionSummary = Get-SidecarReportSummary -Root $root -Name "sidecar_regression_quality_report"
$sidecarConsistencySummary = Get-SidecarReportSummary -Root $root -Name "sidecar_python_rust_consistency_report"

Info "running architecture scorecard release gate"
Assert-ArchitectureScorecardReleaseReady $architectureScorecardSummary
$architectureScorecardGateStatus = "passed"
Ok "architecture scorecard release gate passed"

Info "running sidecar regression release gate"
Assert-SidecarReportReady $sidecarRegressionSummary "sidecar regression release gate"
$sidecarRegressionGateStatus = "passed"
Ok "sidecar regression release gate passed"

Info "running sidecar python/rust consistency release gate"
Assert-SidecarReportReady $sidecarConsistencySummary "sidecar python/rust consistency release gate" -RequireNoSkipped
$sidecarConsistencyGateStatus = "passed"
Ok "sidecar python/rust consistency release gate passed"

if (-not $SkipFrontendConvergenceGate) {
  Info "running frontend convergence release gate"
  powershell -ExecutionPolicy Bypass -File $frontendGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by frontend convergence gate" }
  $frontendGateStatus = "passed"
  Ok "frontend convergence release gate passed"
}

if (-not $SkipWorkflowContractSyncGate) {
  Info "running workflow contract release gate"
  powershell -ExecutionPolicy Bypass -File $workflowGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by workflow contract gate" }
  $workflowGateStatus = "passed"
  Ok "workflow contract release gate passed"
}

$governanceCapabilityExportResult = Invoke-GovernanceCapabilityExportStep -ScriptPath $governanceCapabilityExportScript -FailureScope "release"
if (-not [bool]$governanceCapabilityExportResult["ok"]) {
  throw [string]$governanceCapabilityExportResult["failure_message"]
}
$governanceCapabilityExportStatus = "passed"

if (-not $SkipGovernanceControlPlaneBoundaryGate) {
  Info "running governance control plane boundary release gate"
  powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by governance control plane boundary gate" }
  $governanceControlPlaneBoundaryGateStatus = "passed"
  Ok "governance control plane boundary release gate passed"
}

if (-not $SkipOperatorCatalogSyncGate) {
  Info "running operator catalog release gate"
  powershell -ExecutionPolicy Bypass -File $operatorGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by operator catalog gate" }
  $operatorGateStatus = "passed"
  Ok "operator catalog release gate passed"
}

if (-not $SkipFallbackGovernanceGate) {
  Info "running fallback governance release gate"
  powershell -ExecutionPolicy Bypass -File $fallbackGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by fallback governance gate" }
  $fallbackGateStatus = "passed"
  Ok "fallback governance release gate passed"
}

if (-not $SkipGovernanceStoreSchemaVersionsGate) {
  Info "running governance store schema version release gate"
  powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionsGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by governance store schema version gate" }
  $governanceStoreSchemaVersionsGateStatus = "passed"
  Ok "governance store schema version release gate passed"
}

if (-not $SkipLocalWorkflowStoreSchemaVersionsGate) {
  Info "running local workflow store schema version release gate"
  powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionsGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by local workflow store schema version gate" }
  $localWorkflowStoreSchemaVersionsGateStatus = "passed"
  Ok "local workflow store schema version release gate passed"
}

if (-not $SkipTemplatePackContractSyncGate) {
  Info "running template pack contract release gate"
  powershell -ExecutionPolicy Bypass -File $templatePackContractSyncGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by template pack contract gate" }
  $templatePackContractSyncGateStatus = "passed"
  Ok "template pack contract release gate passed"
}

if (-not $SkipLocalTemplateStorageContractSyncGate) {
  Info "running local template storage contract release gate"
  powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by local template storage contract gate" }
  $localTemplateStorageContractSyncGateStatus = "passed"
  Ok "local template storage contract release gate passed"
}

if (-not $SkipOfflineTemplateCatalogSyncGate) {
  Info "running offline template catalog release gate"
  powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncGate
  if ($LASTEXITCODE -ne 0) { throw "release blocked by offline template catalog gate" }
  $offlineTemplateCatalogSyncGateStatus = "passed"
  Ok "offline template catalog release gate passed"
}

if (-not $SkipSqlConnectivityGate) {
  Info "running SQL connectivity release gate"
  powershell -ExecutionPolicy Bypass -File $sqlGate -EnvFile $EnvFile -SkipWhenTaskStoreNotSql
  if ($LASTEXITCODE -ne 0) { throw "release blocked by SQL connectivity gate" }
  $sqlGateStatus = "passed"
  Ok "SQL connectivity release gate passed"
} else {
  Warn "skip SQL connectivity release gate"
}

if ($IncludeMsix -and $ReleaseAudience -eq "PersonalSideload" -and $MsixSigningMode -eq "PersonalSideloadCert") {
  Info "ensuring reusable personal sideload certificate"
  powershell -ExecutionPolicy Bypass -File $ensurePersonalSideloadCert
  if ($LASTEXITCODE -ne 0) { throw "personal sideload certificate ensure failed" }

  Info "checking personal sideload certificate release window"
  $certArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $checkPersonalSideloadCert,
    "-WarnWhenExpiresInDays", "$PersonalSideloadCertWarnWhenExpiresInDays"
  )
  if (-not $AllowExpiringPersonalSideloadCert) {
    $certArgs += @("-FailWhenExpiresInDays", "$PersonalSideloadCertFailWhenExpiresInDays")
  }
  powershell @certArgs
  if ($LASTEXITCODE -ne 0) { throw "personal sideload certificate release gate failed" }
  $personalSideloadCertStatus = "passed"
  Ok "personal sideload certificate release gate passed"
}

$packageArgs = @(
  "-ExecutionPolicy", "Bypass",
  "-File", $packageScript,
  "-Root", $root,
  "-Version", $Version,
  "-Configuration", $Configuration,
  "-ReleaseChannel", $Channel
)
if ($CreateZip) { $packageArgs += "-CreateZip" }
if ($SkipFrontendConvergenceGate) { $packageArgs += "-SkipFrontendConvergenceGate" }
if ($SkipWorkflowContractSyncGate) { $packageArgs += "-SkipWorkflowContractSyncGate" }
if ($SkipGovernanceControlPlaneBoundaryGate) { $packageArgs += "-SkipGovernanceControlPlaneBoundaryGate" }
if ($SkipOperatorCatalogSyncGate) { $packageArgs += "-SkipOperatorCatalogSyncGate" }
if ($SkipFallbackGovernanceGate) { $packageArgs += "-SkipFallbackGovernanceGate" }
if ($SkipGovernanceStoreSchemaVersionsGate) { $packageArgs += "-SkipGovernanceStoreSchemaVersionsGate" }
if ($SkipLocalWorkflowStoreSchemaVersionsGate) { $packageArgs += "-SkipLocalWorkflowStoreSchemaVersionsGate" }
if ($SkipTemplatePackContractSyncGate) { $packageArgs += "-SkipTemplatePackContractSyncGate" }
if ($SkipLocalTemplateStorageContractSyncGate) { $packageArgs += "-SkipLocalTemplateStorageContractSyncGate" }
if ($SkipOfflineTemplateCatalogSyncGate) { $packageArgs += "-SkipOfflineTemplateCatalogSyncGate" }
if ($SkipNativeWinuiSmokeGate) { $packageArgs += "-SkipNativeWinuiSmokeGate" }

Info "packaging primary frontend: WinUI"
$packageOutput = powershell @packageArgs
if ($LASTEXITCODE -ne 0) { throw "native winui package step failed" }
$bundleRoot = (Extract-LastPathLine $packageOutput).Trim()

$auditPath = Join-Path $outRoot ("release_frontend_audit_{0}.json" -f $Version)
$audit = [ordered]@{
  version = $Version
  frontend = "winui"
  channel = $Channel
  release_audience = $ReleaseAudience
  configuration = $Configuration
  generated_at = (Get-Date).ToString("s")
  bundle_root = $bundleRoot
  frontend_verification = Get-FrontendVerificationSummary -Root $root
  architecture_scorecard = $architectureScorecardSummary
  sidecar_regression = $sidecarRegressionSummary
  sidecar_python_rust_consistency = $sidecarConsistencySummary
  gates = [ordered]@{
    architecture_scorecard = $architectureScorecardGateStatus
    sidecar_regression = $sidecarRegressionGateStatus
    sidecar_python_rust_consistency = $sidecarConsistencyGateStatus
    frontend_convergence = $frontendGateStatus
    workflow_contract_sync = $workflowGateStatus
    governance_capability_export = $governanceCapabilityExportStatus
    governance_control_plane_boundary = $governanceControlPlaneBoundaryGateStatus
    operator_catalog_sync = $operatorGateStatus
    fallback_governance = $fallbackGateStatus
    governance_store_schema_versions = $governanceStoreSchemaVersionsGateStatus
    local_workflow_store_schema_versions = $localWorkflowStoreSchemaVersionsGateStatus
    template_pack_contract_sync = $templatePackContractSyncGateStatus
    local_template_storage_contract_sync = $localTemplateStorageContractSyncGateStatus
    offline_template_catalog_sync = $offlineTemplateCatalogSyncGateStatus
    sql_connectivity = $sqlGateStatus
    personal_sideload_certificate = $personalSideloadCertStatus
  }
}
($audit | ConvertTo-Json -Depth 6) | Set-Content $auditPath -Encoding UTF8
Ok ("frontend release audit written: " + $auditPath)
Ok ("primary frontend release ready: " + $bundleRoot)
if ($IncludeMsix) {
  $msixArgs = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $msixScript,
    "-Root", $root,
    "-Version", $Version,
    "-Configuration", $Configuration,
    "-ReleaseAudience", $ReleaseAudience,
    "-SigningMode", $MsixSigningMode
  )
  if ($SkipFrontendConvergenceGate) { $msixArgs += "-SkipFrontendConvergenceGate" }
  if ($SkipWorkflowContractSyncGate) { $msixArgs += "-SkipWorkflowContractSyncGate" }
  if ($SkipGovernanceControlPlaneBoundaryGate) { $msixArgs += "-SkipGovernanceControlPlaneBoundaryGate" }
  if ($SkipOperatorCatalogSyncGate) { $msixArgs += "-SkipOperatorCatalogSyncGate" }
  if ($SkipFallbackGovernanceGate) { $msixArgs += "-SkipFallbackGovernanceGate" }
  if ($SkipGovernanceStoreSchemaVersionsGate) { $msixArgs += "-SkipGovernanceStoreSchemaVersionsGate" }
  if ($SkipLocalWorkflowStoreSchemaVersionsGate) { $msixArgs += "-SkipLocalWorkflowStoreSchemaVersionsGate" }
  if ($SkipTemplatePackContractSyncGate) { $msixArgs += "-SkipTemplatePackContractSyncGate" }
  if ($SkipLocalTemplateStorageContractSyncGate) { $msixArgs += "-SkipLocalTemplateStorageContractSyncGate" }
  if ($SkipOfflineTemplateCatalogSyncGate) { $msixArgs += "-SkipOfflineTemplateCatalogSyncGate" }
  if ($SkipNativeWinuiSmokeGate) { $msixArgs += "-SkipNativeWinuiSmokeGate" }
  if ($KeepGeneratedMsixCertificate) { $msixArgs += "-KeepGeneratedCertificate" }
  if (-not [string]::IsNullOrWhiteSpace($MsixPfxPath)) { $msixArgs += @("-PfxPath", $MsixPfxPath) }
  if (-not [string]::IsNullOrWhiteSpace($MsixPfxPassword)) { $msixArgs += @("-PfxPassword", $MsixPfxPassword) }
  if (-not [string]::IsNullOrWhiteSpace($MsixCertificatePath)) { $msixArgs += @("-CertificatePath", $MsixCertificatePath) }
  if (-not [string]::IsNullOrWhiteSpace($MsixSigningThumbprint)) { $msixArgs += @("-SigningThumbprint", $MsixSigningThumbprint) }
  if (-not [string]::IsNullOrWhiteSpace($MsixTimestampUrl)) { $msixArgs += @("-TimestampUrl", $MsixTimestampUrl) }
  if ($GenerateAppInstaller) { $msixArgs += "-GenerateAppInstaller" }
  if (-not [string]::IsNullOrWhiteSpace($MsixAppInstallerUriBase)) { $msixArgs += @("-AppInstallerUriBase", $MsixAppInstallerUriBase) }
  Info "packaging optional MSIX preview for primary frontend"
  $msixOutput = powershell @msixArgs
  if ($LASTEXITCODE -ne 0) { throw "native winui msix package step failed" }
  $msixPath = (Extract-LastPathLine $msixOutput).Trim()

  $audit.msix = [ordered]@{
    enabled = $true
    path = $msixPath
    signing_mode = $MsixSigningMode
    appinstaller = $GenerateAppInstaller
  }
  ($audit | ConvertTo-Json -Depth 6) | Set-Content $auditPath -Encoding UTF8
  Ok ("optional msix preview ready: " + $msixPath)
}
