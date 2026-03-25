param(
  [Parameter(Mandatory = $true)][string]$Version,
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [string]$ReleaseChannel = "stable",
  [string]$OutDir = "",
  [string]$PublishedDir = "",
  [switch]$SkipPublish,
  [switch]$SkipFrontendConvergenceGate,
  [switch]$SkipWorkflowContractSyncGate,
  [switch]$SkipGovernanceControlPlaneBoundaryGate,
  [switch]$SkipOperatorCatalogSyncGate,
  [switch]$SkipGovernanceStoreSchemaVersionsGate,
  [switch]$SkipLocalWorkflowStoreSchemaVersionsGate,
  [switch]$SkipTemplatePackContractSyncGate,
  [switch]$SkipLocalTemplateStorageContractSyncGate,
  [switch]$SkipOfflineTemplateCatalogSyncGate,
  [switch]$SkipNativeWinuiSmokeGate,
  [switch]$CreateZip,
  [switch]$CleanOldReleases
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Extract-LastPathLine([object]$Output) {
  $text = [string]::Join("`n", @($Output))
  $matches = $text -split "`r?`n" | Where-Object { $_ -match '^[A-Za-z]:\\' }
  $last = $matches | Select-Object -Last 1
  if ($null -eq $last) { return "" }
  return [string]$last
}
. (Join-Path $PSScriptRoot "governance_capability_export_support.ps1")

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$frontendGate = Join-Path $PSScriptRoot "check_frontend_convergence.ps1"
$workflowGate = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceCapabilityExportScript = Join-Path $PSScriptRoot "export_governance_capabilities.ps1"
$governanceControlPlaneBoundaryGate = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$operatorGate = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$governanceStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_governance_store_schema_versions.ps1"
$localWorkflowStoreSchemaVersionsGate = Join-Path $PSScriptRoot "check_local_workflow_store_schema_versions.ps1"
$templatePackContractSyncGate = Join-Path $PSScriptRoot "check_template_pack_contract_sync.ps1"
$localTemplateStorageContractSyncGate = Join-Path $PSScriptRoot "check_local_template_storage_contract_sync.ps1"
$offlineTemplateCatalogSyncGate = Join-Path $PSScriptRoot "check_offline_template_catalog_sync.ps1"
$winuiSmoke = Join-Path $PSScriptRoot "check_native_winui_smoke.ps1"
$publishScript = Join-Path $PSScriptRoot "publish_native_winui.ps1"

$frontendGateStatus = "skipped"
$frontendGateCheckedAt = ""
$frontendGateError = ""
$workflowGateStatus = "skipped"
$workflowGateCheckedAt = ""
$workflowGateError = ""
$governanceCapabilityExportStatus = "skipped"
$governanceCapabilityExportCheckedAt = ""
$governanceCapabilityExportError = ""
$governanceControlPlaneBoundaryGateStatus = "skipped"
$governanceControlPlaneBoundaryGateCheckedAt = ""
$governanceControlPlaneBoundaryGateError = ""
$operatorGateStatus = "skipped"
$operatorGateCheckedAt = ""
$operatorGateError = ""
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
$nativeWinuiSmokeGateStatus = "skipped"
$nativeWinuiSmokeGateCheckedAt = ""
$nativeWinuiSmokeGateError = ""

if (-not $SkipFrontendConvergenceGate) {
  Info "running frontend convergence package gate"
  powershell -ExecutionPolicy Bypass -File $frontendGate
  $frontendGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by frontend convergence gate" }
  $frontendGateStatus = "passed"
  Ok "frontend convergence package gate passed"
} else {
  $frontendGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip frontend convergence package gate" -ForegroundColor Yellow
}
if (-not $SkipWorkflowContractSyncGate) {
  Info "running workflow contract package gate"
  powershell -ExecutionPolicy Bypass -File $workflowGate
  $workflowGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by workflow contract gate" }
  $workflowGateStatus = "passed"
  Ok "workflow contract package gate passed"
} else {
  $workflowGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip workflow contract package gate" -ForegroundColor Yellow
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
  Info "running governance control plane boundary package gate"
  powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryGate
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by governance control plane boundary gate" }
  $governanceControlPlaneBoundaryGateStatus = "passed"
  Ok "governance control plane boundary package gate passed"
} else {
  $governanceControlPlaneBoundaryGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance control plane boundary package gate" -ForegroundColor Yellow
}
if (-not $SkipOperatorCatalogSyncGate) {
  Info "running operator catalog package gate"
  powershell -ExecutionPolicy Bypass -File $operatorGate
  $operatorGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by operator catalog gate" }
  $operatorGateStatus = "passed"
  Ok "operator catalog package gate passed"
} else {
  $operatorGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip operator catalog package gate" -ForegroundColor Yellow
}
if (-not $SkipGovernanceStoreSchemaVersionsGate) {
  Info "running governance store schema version package gate"
  powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionsGate
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by governance store schema version gate" }
  $governanceStoreSchemaVersionsGateStatus = "passed"
  Ok "governance store schema version package gate passed"
} else {
  $governanceStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip governance store schema version package gate" -ForegroundColor Yellow
}
if (-not $SkipLocalWorkflowStoreSchemaVersionsGate) {
  Info "running local workflow store schema version package gate"
  powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionsGate
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by local workflow store schema version gate" }
  $localWorkflowStoreSchemaVersionsGateStatus = "passed"
  Ok "local workflow store schema version package gate passed"
} else {
  $localWorkflowStoreSchemaVersionsGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local workflow store schema version package gate" -ForegroundColor Yellow
}
if (-not $SkipTemplatePackContractSyncGate) {
  Info "running template pack contract package gate"
  powershell -ExecutionPolicy Bypass -File $templatePackContractSyncGate
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by template pack contract gate" }
  $templatePackContractSyncGateStatus = "passed"
  Ok "template pack contract package gate passed"
} else {
  $templatePackContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip template pack contract package gate" -ForegroundColor Yellow
}
if (-not $SkipLocalTemplateStorageContractSyncGate) {
  Info "running local template storage contract package gate"
  powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncGate
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by local template storage contract gate" }
  $localTemplateStorageContractSyncGateStatus = "passed"
  Ok "local template storage contract package gate passed"
} else {
  $localTemplateStorageContractSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip local template storage contract package gate" -ForegroundColor Yellow
}
if (-not $SkipOfflineTemplateCatalogSyncGate) {
  Info "running offline template catalog package gate"
  powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncGate
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by offline template catalog gate" }
  $offlineTemplateCatalogSyncGateStatus = "passed"
  Ok "offline template catalog package gate passed"
} else {
  $offlineTemplateCatalogSyncGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip offline template catalog package gate" -ForegroundColor Yellow
}

if ([string]::IsNullOrWhiteSpace($PublishedDir)) {
  if ($SkipPublish) {
    throw "PublishedDir is required when SkipPublish is set"
  }
  $publishOutput = powershell -ExecutionPolicy Bypass -File $publishScript -Root $Root -Configuration $Configuration -Version $Version
  if ($LASTEXITCODE -ne 0) { throw "native winui publish step failed" }
  $PublishedDir = (Extract-LastPathLine $publishOutput).Trim()
}

if (-not (Test-Path $PublishedDir)) {
  throw "published dir not found: $PublishedDir"
}

$publishedExe = Join-Path $PublishedDir "WinUI3Bootstrap.exe"
if (-not (Test-Path $publishedExe)) {
  throw "published native winui executable not found: $publishedExe"
}

if (-not $SkipNativeWinuiSmokeGate) {
  Info "running native winui package smoke gate"
  powershell -ExecutionPolicy Bypass -File $winuiSmoke -Root $Root -Configuration $Configuration -SkipBuild -ExePath $publishedExe
  $nativeWinuiSmokeGateCheckedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) { throw "package blocked by native winui smoke gate" }
  $nativeWinuiSmokeGateStatus = "passed"
  Ok "native winui package smoke gate passed"
} else {
  $nativeWinuiSmokeGateCheckedAt = (Get-Date).ToString("s")
  Write-Host "[WARN] skip native winui package smoke gate" -ForegroundColor Yellow
}

if (-not $OutDir) {
  $OutDir = Join-Path $Root ("release\native_winui_bundle_{0}" -f $Version)
}

$bundleRoot = Join-Path $OutDir "AIWF_Native_WinUI_Bundle"
$appRoot = Join-Path $bundleRoot "app"
$docsRoot = Join-Path $bundleRoot "docs"
$contractsRoot = Join-Path $bundleRoot "contracts\desktop"
$workflowContractsRoot = Join-Path $bundleRoot "contracts\workflow"
$rustContractsRoot = Join-Path $bundleRoot "contracts\rust"
$governanceContractsRoot = Join-Path $bundleRoot "contracts\governance"
$installScriptSource = Join-Path $PSScriptRoot "install_native_winui_bundle.ps1"
$uninstallScriptSource = Join-Path $PSScriptRoot "uninstall_native_winui_bundle.ps1"
$installScriptTarget = Join-Path $bundleRoot "Install_AIWF_Native_WinUI.ps1"
$uninstallScriptTarget = Join-Path $bundleRoot "Uninstall_AIWF_Native_WinUI.ps1"
$installCmdTarget = Join-Path $bundleRoot "Install_AIWF_Native_WinUI.cmd"
$installManifestPath = Join-Path $bundleRoot "install_manifest.json"

if ($CleanOldReleases) {
  $releaseRoot = Join-Path $Root "release"
  if (Test-Path $releaseRoot) {
    Get-ChildItem $releaseRoot -Directory -Filter "native_winui_bundle_*" |
      Where-Object { $_.FullName -ne $OutDir } |
      ForEach-Object { Remove-Item $_.FullName -Recurse -Force }
  }
}

if (Test-Path $bundleRoot) {
  Remove-Item $bundleRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $appRoot -Force | Out-Null
New-Item -ItemType Directory -Path $docsRoot -Force | Out-Null
New-Item -ItemType Directory -Path $contractsRoot -Force | Out-Null
New-Item -ItemType Directory -Path $workflowContractsRoot -Force | Out-Null
New-Item -ItemType Directory -Path $rustContractsRoot -Force | Out-Null
New-Item -ItemType Directory -Path $governanceContractsRoot -Force | Out-Null

Info "copying published native winui app"
Copy-Item (Join-Path $PublishedDir "*") $appRoot -Recurse -Force
Copy-Item $installScriptSource $installScriptTarget -Force
Copy-Item $uninstallScriptSource $uninstallScriptTarget -Force

$docList = @(
  "docs\quickstart_native_winui.md",
  "docs\offline_delivery_native_winui.md",
  "docs\frontend_convergence_decision_20260320.md",
  "apps\dify-native-winui\README.md",
  "apps\dify-native-winui\IPC_BRIDGE_CONTRACT.md"
)
foreach ($docRel in $docList) {
  $src = Join-Path $Root $docRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $docsRoot (Split-Path $src -Leaf))
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
  $src = Join-Path $Root $schemaRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $contractsRoot (Split-Path $src -Leaf))
  }
}

$workflowContractFiles = @(
  "contracts\workflow\workflow.schema.json",
  "contracts\workflow\render_contract.schema.json",
  "contracts\workflow\minimal_workflow.v1.json"
)
foreach ($contractRel in $workflowContractFiles) {
  $src = Join-Path $Root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $workflowContractsRoot (Split-Path $src -Leaf))
  }
}

$rustContractFiles = @(
  "contracts\rust\operators_manifest.v1.json",
  "contracts\rust\operators_manifest.schema.json",
  "contracts\rust\operators_extension_v1.schema.json",
  "contracts\rust\transform_rows_v2.schema.json"
)
foreach ($contractRel in $rustContractFiles) {
  $src = Join-Path $Root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $rustContractsRoot (Split-Path $src -Leaf))
  }
}

$governanceContractFiles = @(
  "contracts\governance\governance_capabilities.v1.json"
)
foreach ($contractRel in $governanceContractFiles) {
  $src = Join-Path $Root $contractRel
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $governanceContractsRoot (Split-Path $src -Leaf))
  }
}

$installCmd = @(
  "@echo off",
  "powershell -ExecutionPolicy Bypass -File ""%~dp0Install_AIWF_Native_WinUI.ps1"" %*"
)
$installCmd | Set-Content $installCmdTarget -Encoding ASCII

$installManifest = [ordered]@{
  product = "AIWF Native WinUI"
  default_install_root = "%LOCALAPPDATA%\Programs\AIWF\NativeWinUI"
  installer_script = "Install_AIWF_Native_WinUI.ps1"
  uninstall_script = "Uninstall_AIWF_Native_WinUI.ps1"
  launcher = "app/WinUI3Bootstrap.exe"
}
($installManifest | ConvertTo-Json -Depth 4) | Set-Content $installManifestPath -Encoding UTF8

$readmeLines = @(
  "# AIWF Native WinUI Bundle",
  "",
  "Version: $Version",
  "Channel: $ReleaseChannel",
  "Frontend: WinUI",
  "",
  "## Run",
  "1. Run Install_AIWF_Native_WinUI.cmd or Install_AIWF_Native_WinUI.ps1.",
  "2. Installer copies the bundle into LocalAppData\\Programs\\AIWF\\NativeWinUI.",
  "3. Launch AIWF Native WinUI from Start Menu or app\\WinUI3Bootstrap.exe.",
  "",
  "## Contents",
  "- app/",
  "- docs/",
  "- contracts/desktop/",
  "- contracts/workflow/",
  "- contracts/rust/",
  "- contracts/governance/",
  "- Install_AIWF_Native_WinUI.ps1",
  "- Uninstall_AIWF_Native_WinUI.ps1",
  "- Install_AIWF_Native_WinUI.cmd",
  "- install_manifest.json",
  "- manifest.json",
  "- RELEASE_NOTES.md",
  "- SHA256SUMS.txt"
)
$readmeLines | Set-Content (Join-Path $bundleRoot "README.txt") -Encoding UTF8

$manifest = [ordered]@{
  product = "AIWF Native WinUI"
  version = $Version
  release_channel = $ReleaseChannel
  frontend = "winui"
  configuration = $Configuration
  generated_at = (Get-Date).ToString("s")
  published_dir = $PublishedDir
  entry = "app/WinUI3Bootstrap.exe"
  docs = @((Get-ChildItem $docsRoot -File | ForEach-Object { $_.Name }))
  contract_schemas = @((Get-ChildItem $contractsRoot -File | ForEach-Object { ("contracts/desktop/" + $_.Name) }))
  workflow_contracts = @((Get-ChildItem $workflowContractsRoot -File | ForEach-Object { ("contracts/workflow/" + $_.Name) }))
  rust_contracts = @((Get-ChildItem $rustContractsRoot -File | ForEach-Object { ("contracts/rust/" + $_.Name) }))
  governance_contracts = @((Get-ChildItem $governanceContractsRoot -File | ForEach-Object { ("contracts/governance/" + $_.Name) }))
  gates = [ordered]@{
    frontend_convergence = [ordered]@{
      status = $frontendGateStatus
      checked_at = $frontendGateCheckedAt
      script = "ops/scripts/check_frontend_convergence.ps1"
      error = $frontendGateError
    }
    workflow_contract_sync = [ordered]@{
      status = $workflowGateStatus
      checked_at = $workflowGateCheckedAt
      script = "ops/scripts/check_workflow_contract_sync.ps1"
      error = $workflowGateError
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
      status = $operatorGateStatus
      checked_at = $operatorGateCheckedAt
      script = "ops/scripts/check_operator_catalog_sync.ps1"
      error = $operatorGateError
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
    native_winui_smoke = [ordered]@{
      status = $nativeWinuiSmokeGateStatus
      checked_at = $nativeWinuiSmokeGateCheckedAt
      script = "ops/scripts/check_native_winui_smoke.ps1"
      error = $nativeWinuiSmokeGateError
    }
  }
}
($manifest | ConvertTo-Json -Depth 6) | Set-Content (Join-Path $bundleRoot "manifest.json") -Encoding UTF8

$notes = @(
  "# Release Notes",
  "",
  "- Version: $Version",
  "- Channel: $ReleaseChannel",
  "- Frontend: WinUI",
  "- GeneratedAt: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))",
  "",
  "This bundle contains the primary WinUI desktop frontend."
)
$notes | Set-Content (Join-Path $bundleRoot "RELEASE_NOTES.md") -Encoding UTF8

$allFiles = Get-ChildItem -Path $bundleRoot -Recurse -File | Sort-Object FullName
$shaLines = @()
foreach ($file in $allFiles) {
  $relative = $file.FullName.Substring($bundleRoot.Length).TrimStart('\')
  $hash = (Get-FileHash $file.FullName -Algorithm SHA256).Hash
  $shaLines += ("{0}  {1}" -f $hash, $relative)
}
$shaLines | Set-Content (Join-Path $bundleRoot "SHA256SUMS.txt") -Encoding ASCII

if ($CreateZip) {
  $zipPath = Join-Path $OutDir "AIWF_Native_WinUI_Bundle.zip"
  if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
  Compress-Archive -Path (Join-Path $bundleRoot "*") -DestinationPath $zipPath
  Ok ("native winui bundle zip created: " + $zipPath)
}

Ok ("native winui bundle ready: " + $bundleRoot)
Write-Output $bundleRoot
