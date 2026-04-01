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
  [switch]$SkipCleaningRustV2RolloutGate,
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

function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$legacy = Join-Path $PSScriptRoot "release_productize.ps1"
Warn "Electron compatibility release path invoked. WinUI is the primary frontend; use release_frontend_productize.ps1 for the default release path."

& $legacy `
  -Version $Version `
  -Channel $Channel `
  -IncludeBundledTools:$IncludeBundledTools `
  -CollectBundledTools:$CollectBundledTools `
  -EnvFile $EnvFile `
  -SkipSqlConnectivityGate:$SkipSqlConnectivityGate `
  -SkipRoutingBenchGate:$SkipRoutingBenchGate `
  -SkipRustTransformBenchGate:$SkipRustTransformBenchGate `
  -SkipOpenApiSdkSyncGate:$SkipOpenApiSdkSyncGate `
  -SkipWorkflowContractSyncGate:$SkipWorkflowContractSyncGate `
  -SkipGovernanceControlPlaneBoundaryGate:$SkipGovernanceControlPlaneBoundaryGate `
  -SkipOperatorCatalogSyncGate:$SkipOperatorCatalogSyncGate `
  -SkipFallbackGovernanceGate:$SkipFallbackGovernanceGate `
  -SkipCleaningRustV2RolloutGate:$SkipCleaningRustV2RolloutGate `
  -SkipGovernanceStoreSchemaVersionsGate:$SkipGovernanceStoreSchemaVersionsGate `
  -SkipLocalWorkflowStoreSchemaVersionsGate:$SkipLocalWorkflowStoreSchemaVersionsGate `
  -SkipTemplatePackContractSyncGate:$SkipTemplatePackContractSyncGate `
  -SkipLocalTemplateStorageContractSyncGate:$SkipLocalTemplateStorageContractSyncGate `
  -SkipOfflineTemplateCatalogSyncGate:$SkipOfflineTemplateCatalogSyncGate `
  -RustBenchRows $RustBenchRows `
  -RustBenchRuns $RustBenchRuns `
  -RustBenchWarmup $RustBenchWarmup `
  -RustBenchMinSpeedup $RustBenchMinSpeedup `
  -RustBenchMinArrowSpeedup $RustBenchMinArrowSpeedup `
  -RustBenchUpdateProfileOnPass:$RustBenchUpdateProfileOnPass
exit $LASTEXITCODE
