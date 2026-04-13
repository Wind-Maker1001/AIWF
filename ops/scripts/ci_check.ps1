param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [ValidateSet("Default","Quick","Full","Compatibility")]
  [string]$CiProfile = "Default",
  [switch]$SkipToolChecks,
  [switch]$SkipDocsChecks,
  [switch]$SkipEncodingChecks,
  [switch]$SkipJavaTests,
  [switch]$SkipRustTests,
  [switch]$SkipPythonTests,
  [switch]$SkipRegressionQuality,
  [switch]$SkipSidecarRegressionQuality,
  [switch]$SkipSidecarPythonRustConsistency,
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
  [switch]$SkipFrontendConvergence,
  [switch]$SkipWorkflowContractSync,
  [switch]$SkipGovernanceControlPlaneBoundary,
  [switch]$SkipGovernanceStoreSchemaVersions,
  [switch]$SkipLocalWorkflowStoreSchemaVersions,
  [switch]$SkipTemplatePackContractSync,
  [switch]$SkipLocalTemplateStorageContractSync,
  [switch]$SkipOfflineTemplateCatalogSync,
  [switch]$SkipNodeConfigSchemaCoverage,
  [switch]$SkipLocalNodeCatalogPolicy,
  [switch]$SkipOperatorCatalogSync,
  [switch]$SkipCleaningRustV2RolloutGate,
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
function ApplyProfileSkips([string]$ProfileLabel, [string[]]$ParamNames, [hashtable]$BoundParams) {
  $applied = @()
  foreach ($paramName in $ParamNames) {
    if ($BoundParams.ContainsKey($paramName)) { continue }
    Set-Variable -Name $paramName -Value $true -Scope Script
    $applied += $paramName
  }

  if ($applied.Count -gt 0) {
    $labels = $applied | ForEach-Object { ($_ -replace "^Skip", "") }
    Info ("{0} profile auto-skips: {1}" -f $ProfileLabel, ($labels -join ", "))
  }
}
function ApplyCiProfile([string]$ProfileName, [hashtable]$BoundParams) {
  $normalized = "default"
  if (-not [string]::IsNullOrWhiteSpace($ProfileName)) {
    $normalized = $ProfileName.Trim().ToLowerInvariant()
  }

  if ($normalized -notin @("default", "quick", "full", "compatibility")) {
    throw "unsupported ci profile: $ProfileName"
  }

  if ($normalized -ne "default") {
    Info "using ci profile: $normalized"
  }

  if ($normalized -in @("default", "full")) {
    ApplyProfileSkips -ProfileLabel "default/full" -ParamNames @("SkipDesktopPackageTests") -BoundParams $BoundParams
    if (-not $BoundParams.ContainsKey("SkipDesktopPackageTests")) {
      Info "Electron compatibility packaged startup checks moved to the explicit compatibility stage. Use -CiProfile Compatibility when you are changing Electron packaging paths."
    }
    return
  }

  if ($normalized -eq "quick") {
  $quickSkipParams = @(
    "SkipRegressionQuality",
    "SkipSidecarRegressionQuality",
    "SkipSidecarPythonRustConsistency",
    "SkipDesktopRealSampleAcceptance",
    "SkipDesktopFinanceTemplateAcceptance",
    "SkipDesktopStress",
    "SkipDesktopPackageTests",
    "SkipRoutingBench",
    "SkipAsyncBench",
    "SkipRustTransformBenchGate",
    "SkipRustNewOpsBenchGate",
    "SkipContractTests",
    "SkipChaosChecks",
    "SkipSmoke",
    "SkipCleaningRustV2RolloutGate"
  )
    ApplyProfileSkips -ProfileLabel "quick" -ParamNames $quickSkipParams -BoundParams $BoundParams
    return
  }

  $compatibilitySkipParams = @(
    "SkipJavaTests",
    "SkipRustTests",
    "SkipPythonTests",
    "SkipRegressionQuality",
    "SkipSidecarRegressionQuality",
    "SkipSidecarPythonRustConsistency",
    "SkipDesktopUiTests",
    "SkipDesktopRealSampleAcceptance",
    "SkipDesktopFinanceTemplateAcceptance",
    "SkipDesktopStress",
    "SkipRoutingBench",
    "SkipAsyncBench",
    "SkipRustTransformBenchGate",
    "SkipRustNewOpsBenchGate",
    "SkipRegressionBaselineGate",
    "SkipContractTests",
    "SkipChaosChecks",
    "SkipSmoke",
    "SkipSqlConnectivityGate",
    "SkipNativeWinuiSmoke"
  )
  ApplyProfileSkips -ProfileLabel "compatibility" -ParamNames $compatibilitySkipParams -BoundParams $BoundParams
}
function Test-IsCiEnvironment() {
  return [string]::Equals($env:CI, "true", [System.StringComparison]::OrdinalIgnoreCase) -or `
    [string]::Equals($env:GITHUB_ACTIONS, "true", [System.StringComparison]::OrdinalIgnoreCase)
}
function New-FrontendCheckState([string]$Status, [string]$Reason = "") {
  return [ordered]@{
    status = $Status
    reason = $Reason
    updated_at = (Get-Date).ToString("s")
  }
}
function Set-FrontendCheckState($State, [string]$Status, [string]$Reason = "") {
  $State.status = $Status
  $State.reason = $Reason
  $State.updated_at = (Get-Date).ToString("s")
}
function Resolve-FrontendEvidenceOverall([object[]]$Checks) {
  $states = @($Checks | ForEach-Object { [string]($_.status) })
  if ($states -contains "failed") { return "failed" }
  if ($states -contains "passed") { return "passed" }
  if ($states -contains "running") { return "running" }
  if ($states -contains "pending") { return "pending" }
  return "skipped"
}
function Resolve-ArchitectureScorecardOverall([object[]]$Checks) {
  $states = @($Checks | ForEach-Object { [string]($_.status) })
  if ($states -contains "failed") { return "failed" }
  if ($states -contains "pending") { return "pending" }
  if ($states -contains "passed") { return "passed" }
  if ($states -contains "running") { return "running" }
  return "skipped"
}
function Get-ArchitectureScorecardMarkdownLines($Payload) {
  $lines = @()
  $lines += "# Architecture Scorecard"
  $lines += ""
  $lines += "- generated_at: $($Payload.generated_at)"
  $lines += "- profile: $($Payload.profile)"
  $lines += "- overall_status: $($Payload.overall_status)"
  $lines += ""
  $lines += "| Boundary | Status | Reason |"
  $lines += "|---|---|---|"
  foreach ($pair in @(
    @{ name = "workflow_contract_sync"; item = $Payload.boundaries.workflow_contract_sync },
    @{ name = "governance_control_plane_boundary"; item = $Payload.boundaries.governance_control_plane_boundary },
    @{ name = "governance_store_schema_versions"; item = $Payload.boundaries.governance_store_schema_versions },
    @{ name = "local_workflow_store_schema_versions"; item = $Payload.boundaries.local_workflow_store_schema_versions },
    @{ name = "template_pack_contract_sync"; item = $Payload.boundaries.template_pack_contract_sync },
    @{ name = "local_template_storage_contract_sync"; item = $Payload.boundaries.local_template_storage_contract_sync },
    @{ name = "offline_template_catalog_sync"; item = $Payload.boundaries.offline_template_catalog_sync },
    @{ name = "node_config_schema_coverage"; item = $Payload.boundaries.node_config_schema_coverage },
    @{ name = "local_node_catalog_policy"; item = $Payload.boundaries.local_node_catalog_policy },
    @{ name = "operator_catalog_sync"; item = $Payload.boundaries.operator_catalog_sync },
    @{ name = "cleaning_rust_v2_rollout"; item = $Payload.boundaries.cleaning_rust_v2_rollout },
    @{ name = "frontend_convergence"; item = $Payload.boundaries.frontend_convergence },
    @{ name = "frontend_primary_verification"; item = $Payload.frontend.primary },
    @{ name = "frontend_compatibility_verification"; item = $Payload.frontend.compatibility }
  )) {
    $reason = [string]($pair.item.reason)
    if ([string]::IsNullOrWhiteSpace($reason)) {
      $reason = [string]($pair.item.evidence_path)
    }
    $safeReason = $reason -replace "\|", "\|"
    $lines += "| $($pair.name) | $([string]($pair.item.status)) | $safeReason |"
  }
  return ,$lines
}
function Write-FrontendEvidenceSnapshot([string]$Name, $Payload, [string]$Directory, [string]$Stamp) {
  New-Item -ItemType Directory -Path $Directory -Force | Out-Null
  $timestampedPath = Join-Path $Directory ("{0}_{1}.json" -f $Name, $Stamp)
  $latestPath = Join-Path $Directory ("{0}_latest.json" -f $Name)
  $json = ($Payload | ConvertTo-Json -Depth 8)
  Set-Content -Path $timestampedPath -Value $json -Encoding UTF8
  Set-Content -Path $latestPath -Value $json -Encoding UTF8
  return [ordered]@{
    latest = $latestPath
    snapshot = $timestampedPath
  }
}
function Write-ArchitectureScorecardSnapshot($Payload, [string]$Directory, [string]$Stamp, [string]$ProfileLabel) {
  New-Item -ItemType Directory -Path $Directory -Force | Out-Null
  $jsonLatestPath = Join-Path $Directory "architecture_scorecard_latest.json"
  $mdLatestPath = Join-Path $Directory "architecture_scorecard_latest.md"
  $jsonSnapshotPath = Join-Path $Directory ("architecture_scorecard_{0}.json" -f $Stamp)
  $mdSnapshotPath = Join-Path $Directory ("architecture_scorecard_{0}.md" -f $Stamp)
  $safeProfile = [string]($ProfileLabel.ToLowerInvariant())
  $profileJsonLatestPath = Join-Path $Directory ("architecture_scorecard_{0}_latest.json" -f $safeProfile)
  $profileMdLatestPath = Join-Path $Directory ("architecture_scorecard_{0}_latest.md" -f $safeProfile)
  $profileJsonSnapshotPath = Join-Path $Directory ("architecture_scorecard_{0}_{1}.json" -f $safeProfile, $Stamp)
  $profileMdSnapshotPath = Join-Path $Directory ("architecture_scorecard_{0}_{1}.md" -f $safeProfile, $Stamp)

  $json = ($Payload | ConvertTo-Json -Depth 10)
  Set-Content -Path $jsonLatestPath -Value $json -Encoding UTF8
  Set-Content -Path $jsonSnapshotPath -Value $json -Encoding UTF8
  Set-Content -Path $profileJsonLatestPath -Value $json -Encoding UTF8
  Set-Content -Path $profileJsonSnapshotPath -Value $json -Encoding UTF8

  $md = (Get-ArchitectureScorecardMarkdownLines $Payload) -join [Environment]::NewLine
  Set-Content -Path $mdLatestPath -Value $md -Encoding UTF8
  Set-Content -Path $mdSnapshotPath -Value $md -Encoding UTF8
  Set-Content -Path $profileMdLatestPath -Value $md -Encoding UTF8
  Set-Content -Path $profileMdSnapshotPath -Value $md -Encoding UTF8

  return [ordered]@{
    json_latest = $jsonLatestPath
    json_snapshot = $jsonSnapshotPath
    md_latest = $mdLatestPath
    md_snapshot = $mdSnapshotPath
    profile_json_latest = $profileJsonLatestPath
    profile_json_snapshot = $profileJsonSnapshotPath
    profile_md_latest = $profileMdLatestPath
    profile_md_snapshot = $profileMdSnapshotPath
  }
}
function Read-ArchitectureScorecardPayload([string]$Path) {
  if (-not (Test-Path $Path)) { return $null }
  try {
    return Get-Content -Raw -Encoding UTF8 $Path | ConvertFrom-Json
  } catch {
    return $null
  }
}
function Parse-ScorecardTimestamp([string]$Value) {
  if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
  try {
    return [datetime]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind)
  } catch {
    try {
      return [datetime]::Parse($Value)
    } catch {
      return $null
    }
  }
}
function Get-OptionalPropertyValue($Object, [string]$Name) {
  if ($null -eq $Object) { return "" }
  $property = $Object.PSObject.Properties[$Name]
  if ($null -eq $property) { return "" }
  return [string]$property.Value
}
function Get-OptionalObjectProperty($Object, [string]$Name) {
  if ($null -eq $Object) { return $null }
  $property = $Object.PSObject.Properties[$Name]
  if ($null -eq $property) { return $null }
  return $property.Value
}
function Get-OptionalBooleanProperty($Object, [string]$Name) {
  $value = Get-OptionalObjectProperty $Object $Name
  if ($null -eq $value) { return $false }
  if ($value -is [bool]) { return [bool]$value }
  $text = [string]$value
  return [string]::Equals($text, "true", [System.StringComparison]::OrdinalIgnoreCase)
}
function Resolve-ScorecardItemGeneratedAt($Payload, $Item) {
  foreach ($candidate in @(
    (Get-OptionalPropertyValue $Item "updated_at"),
    (Get-OptionalPropertyValue $Item "generated_at"),
    (Get-OptionalPropertyValue $Payload "generated_at")
  )) {
    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
      return [string]$candidate
    }
  }
  return ""
}
function Parse-JsonLineFromOutput([object[]]$Output) {
  $lines = @($Output | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ })
  for ($i = $lines.Count - 1; $i -ge 0; $i--) {
    $line = [string]$lines[$i]
    if ($line.StartsWith("{") -and $line.EndsWith("}")) {
      try {
        return ($line | ConvertFrom-Json)
      } catch {}
    }
  }
  return $null
}
function Get-StringArrayProperty($Object, [string]$Name) {
  $value = Get-OptionalObjectProperty $Object $Name
  if ($null -eq $value) { return @() }
  return @($value | ForEach-Object { [string]$_ } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}
function New-WorkflowContractSyncDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  return [ordered]@{
    required = @(Get-StringArrayProperty $Summary "required")
    default_version = Get-OptionalPropertyValue $Summary "defaultVersion"
    normalized_version = Get-OptionalPropertyValue $Summary "normalizedVersion"
    import_migrated = (Get-OptionalBooleanProperty $Summary "importMigrated")
    import_rejected_unknown_type = (Get-OptionalBooleanProperty $Summary "importRejectedUnknownType")
    payload_rejected_unknown_type = (Get-OptionalBooleanProperty $Summary "payloadRejectedUnknownType")
    authoring_rejected_unknown_type = (Get-OptionalBooleanProperty $Summary "authoringRejectedUnknownType")
    preflight_unknown_type_guided = (Get-OptionalBooleanProperty $Summary "preflightUnknownTypeGuided")
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-WorkflowContractSyncReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "workflow contract sync checks failed" }
    return "workflow contract sync checks passed"
  }

  $details = New-WorkflowContractSyncDetails $Summary
  $required = @($details.required)
  $base = "workflow contract sync checks {0} (required={1}; default_version={2}; normalized_version={3}; import_migrated={4}; import_unknown_type={5}; run_unknown_type={6}; authoring_unknown_type={7}; preflight_unknown_type_guided={8})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    $(if ($required.Count -gt 0) { $required -join ", " } else { "-" }),
    [string]$details.default_version,
    [string]$details.normalized_version,
    [string]$details.import_migrated,
    [string]$details.import_rejected_unknown_type,
    [string]$details.payload_rejected_unknown_type,
    [string]$details.authoring_rejected_unknown_type,
    [string]$details.preflight_unknown_type_guided

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  $requiredMissing = @("workflow_id", "version", "nodes", "edges" | Where-Object { $_ -notin $required })
  if ($requiredMissing.Count -gt 0) {
    $fragments += "required fields missing: $($requiredMissing -join ', ')"
  }
  foreach ($entry in @(
    @{ ok = [bool]$details.import_rejected_unknown_type; label = "import unknown node guard missing" },
    @{ ok = [bool]$details.payload_rejected_unknown_type; label = "run payload unknown node guard missing" },
    @{ ok = [bool]$details.authoring_rejected_unknown_type; label = "authoring unknown node guard missing" },
    @{ ok = [bool]$details.preflight_unknown_type_guided; label = "preflight unknown node guidance missing" }
  )) {
    if (-not $entry.ok) {
      $fragments += [string]$entry.label
    }
  }
  if ([string]::IsNullOrWhiteSpace([string]$details.default_version)) {
    $fragments += "default workflow version missing"
  }
  if ([string]::IsNullOrWhiteSpace([string]$details.normalized_version)) {
    $fragments += "normalized workflow version missing"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-WorkflowContractReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  $required = @(Get-StringArrayProperty $details "required")
  $requiredMissing = @("workflow_id", "version", "nodes", "edges" | Where-Object { $_ -notin $required })
  if ($requiredMissing.Count -gt 0) {
    $issues += "workflow_contract_sync required fields missing: $($requiredMissing -join ', ')"
  }
  if (-not (Get-OptionalBooleanProperty $details "import_rejected_unknown_type")) {
    $issues += "workflow_contract_sync import path no longer rejects unknown node types"
  }
  if (-not (Get-OptionalBooleanProperty $details "payload_rejected_unknown_type")) {
    $issues += "workflow_contract_sync run payload path no longer rejects unknown node types"
  }
  if (-not (Get-OptionalBooleanProperty $details "authoring_rejected_unknown_type")) {
    $issues += "workflow_contract_sync authoring surface no longer rejects unknown node types"
  }
  if (-not (Get-OptionalBooleanProperty $details "preflight_unknown_type_guided")) {
    $issues += "workflow_contract_sync preflight no longer emits unknown node type guidance"
  }
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "default_version"))) {
    $issues += "workflow_contract_sync default workflow version missing"
  }
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "normalized_version"))) {
    $issues += "workflow_contract_sync normalized workflow version missing"
  }
  return @($issues | Sort-Object -Unique)
}
function New-GovernanceControlPlaneBoundaryDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $drift = Get-OptionalObjectProperty $Summary "drift"
  return [ordered]@{
    schema_version = Get-OptionalPropertyValue $Summary "schemaVersion"
    control_plane_status = Get-OptionalPropertyValue $Summary "controlPlaneStatus"
    control_plane_role = Get-OptionalPropertyValue $Summary "controlPlaneRole"
    governance_state_control_plane_owner = Get-OptionalPropertyValue $Summary "governanceStateControlPlaneOwner"
    job_lifecycle_control_plane_owner = Get-OptionalPropertyValue $Summary "jobLifecycleControlPlaneOwner"
    meta_route = Get-OptionalPropertyValue $Summary "metaRoute"
    manifest_path = Get-OptionalPropertyValue $Summary "manifestPath"
    desktop_generated_path = Get-OptionalPropertyValue $Summary "desktopGeneratedPath"
    winui_generated_path = Get-OptionalPropertyValue $Summary "winUiGeneratedPath"
    surface_count = [int]($Summary.surfaceCount)
    governance_route_count = [int]($Summary.governanceRouteCount)
    covered_governance_route_count = [int]($Summary.coveredGovernanceRouteCount)
    drift = [ordered]@{
      missing_required_fields = @(Get-StringArrayProperty $drift "missingRequiredFields")
      invalid_control_plane_roles = @(Get-StringArrayProperty $drift "invalidControlPlaneRoles")
      invalid_state_owners = @(Get-StringArrayProperty $drift "invalidStateOwners")
      invalid_lifecycle_owners = @(Get-StringArrayProperty $drift "invalidLifecycleOwners")
      lifecycle_mutation_allowed = @(Get-StringArrayProperty $drift "lifecycleMutationAllowed")
      invalid_owned_route_prefixes = @(Get-StringArrayProperty $drift "invalidOwnedRoutePrefixes")
      duplicate_owned_route_prefixes = @(Get-StringArrayProperty $drift "duplicateOwnedRoutePrefixes")
      uncovered_governance_routes = @(Get-StringArrayProperty $drift "uncoveredGovernanceRoutes")
      missing_required_routes = @(Get-StringArrayProperty $drift "missingRequiredRoutes")
      capability_map_drift = @(Get-StringArrayProperty $drift "capabilityMapDrift")
      manifest_constant_drift = @(Get-StringArrayProperty $drift "manifestConstantDrift")
      manifest_capability_drift = @(Get-StringArrayProperty $drift "manifestCapabilityDrift")
      manifest_route_prefix_drift = @(Get-StringArrayProperty $drift "manifestRoutePrefixDrift")
      manifest_owned_route_prefix_drift = @(Get-StringArrayProperty $drift "manifestOwnedRoutePrefixDrift")
      manifest_source_authority_drift = @(Get-StringArrayProperty $drift "manifestSourceAuthorityDrift")
      desktop_generated_capability_drift = @(Get-StringArrayProperty $drift "desktopGeneratedCapabilityDrift")
      desktop_generated_route_prefix_drift = @(Get-StringArrayProperty $drift "desktopGeneratedRoutePrefixDrift")
      winui_generated_capability_drift = @(Get-StringArrayProperty $drift "winUiGeneratedCapabilityDrift")
      winui_generated_route_prefix_drift = @(Get-StringArrayProperty $drift "winUiGeneratedRoutePrefixDrift")
      generated_source_authority_drift = @(Get-StringArrayProperty $drift "generatedSourceAuthorityDrift")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-GovernanceControlPlaneBoundaryReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "governance control plane boundary checks failed" }
    return "governance control plane boundary checks passed"
  }

  $details = New-GovernanceControlPlaneBoundaryDetails $Summary
  $base = "governance control plane boundary checks {0} (surfaces={1}; routes={2}/{3}; role={4}; meta={5}; manifest=governance_capabilities.v1.json; desktop_generated=workflow_governance_capabilities.generated.js; winui_generated=GovernanceCapabilities.Generated.cs)" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.surface_count,
    [int]$details.covered_governance_route_count,
    [int]$details.governance_route_count,
    [string]$details.control_plane_role,
    [string]$details.meta_route

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  if ([string]::IsNullOrWhiteSpace([string]$details.schema_version)) {
    $fragments += "schema version missing"
  }
  if ([int]$details.covered_governance_route_count -lt [int]$details.governance_route_count) {
    $fragments += "uncovered governance routes present"
  }
  foreach ($entry in @(
    @{ name = "missing_required_fields"; label = "surface metadata fields missing" },
    @{ name = "invalid_control_plane_roles"; label = "control plane role drift" },
    @{ name = "invalid_state_owners"; label = "state owner drift" },
    @{ name = "invalid_lifecycle_owners"; label = "job lifecycle owner drift" },
    @{ name = "lifecycle_mutation_allowed"; label = "lifecycle mutation exposure drift" },
    @{ name = "invalid_owned_route_prefixes"; label = "invalid owned route prefixes" },
    @{ name = "duplicate_owned_route_prefixes"; label = "duplicate owned route prefixes" },
    @{ name = "uncovered_governance_routes"; label = "uncovered governance routes" },
    @{ name = "missing_required_routes"; label = "required governance routes missing" },
    @{ name = "capability_map_drift"; label = "governance capability map drift" },
    @{ name = "manifest_constant_drift"; label = "governance capability manifest constant drift" },
    @{ name = "manifest_capability_drift"; label = "governance capability manifest capability drift" },
    @{ name = "manifest_route_prefix_drift"; label = "governance capability manifest route prefix drift" },
    @{ name = "manifest_owned_route_prefix_drift"; label = "governance capability manifest owned route prefix drift" },
    @{ name = "manifest_source_authority_drift"; label = "governance capability manifest source authority drift" },
    @{ name = "desktop_generated_capability_drift"; label = "desktop governance capability generated drift" },
    @{ name = "desktop_generated_route_prefix_drift"; label = "desktop governance capability route prefix drift" },
    @{ name = "winui_generated_capability_drift"; label = "winui governance capability generated drift" },
    @{ name = "winui_generated_route_prefix_drift"; label = "winui governance capability route prefix drift" },
    @{ name = "generated_source_authority_drift"; label = "governance capability generated source authority drift" }
  )) {
    $values = @(Get-StringArrayProperty $details.drift $entry.name)
    if ($values.Count -gt 0) {
      $fragments += [string]$entry.label
    }
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-GovernanceControlPlaneBoundaryReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "schema_version"))) {
    $issues += "governance_control_plane_boundary schema version missing"
  }
  $routeCount = [int](Get-OptionalPropertyValue $details "governance_route_count")
  $coveredRouteCount = [int](Get-OptionalPropertyValue $details "covered_governance_route_count")
  if ($routeCount -gt 0 -and $coveredRouteCount -lt $routeCount) {
    $issues += "governance_control_plane_boundary uncovered governance routes: $coveredRouteCount/$routeCount"
  }
  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "missing_required_fields"; label = "surface metadata fields missing" },
    @{ name = "invalid_control_plane_roles"; label = "control plane role drift" },
    @{ name = "invalid_state_owners"; label = "state owner drift" },
    @{ name = "invalid_lifecycle_owners"; label = "job lifecycle owner drift" },
    @{ name = "lifecycle_mutation_allowed"; label = "lifecycle mutation exposure drift" },
    @{ name = "invalid_owned_route_prefixes"; label = "invalid owned route prefixes" },
    @{ name = "duplicate_owned_route_prefixes"; label = "duplicate owned route prefixes" },
    @{ name = "uncovered_governance_routes"; label = "uncovered governance routes" },
    @{ name = "missing_required_routes"; label = "required governance routes missing" },
    @{ name = "capability_map_drift"; label = "governance capability map drift" },
    @{ name = "manifest_constant_drift"; label = "governance capability manifest constant drift" },
    @{ name = "manifest_capability_drift"; label = "governance capability manifest capability drift" },
    @{ name = "manifest_route_prefix_drift"; label = "governance capability manifest route prefix drift" },
    @{ name = "manifest_owned_route_prefix_drift"; label = "governance capability manifest owned route prefix drift" },
    @{ name = "manifest_source_authority_drift"; label = "governance capability manifest source authority drift" },
    @{ name = "desktop_generated_capability_drift"; label = "desktop governance capability generated drift" },
    @{ name = "desktop_generated_route_prefix_drift"; label = "desktop governance capability route prefix drift" },
    @{ name = "winui_generated_capability_drift"; label = "winui governance capability generated drift" },
    @{ name = "winui_generated_route_prefix_drift"; label = "winui governance capability route prefix drift" },
    @{ name = "generated_source_authority_drift"; label = "governance capability generated source authority drift" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $issues += "governance_control_plane_boundary $($entry.label): $($values -join ', ')"
    }
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-GovernanceStoreSchemaVersionDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $drift = Get-OptionalObjectProperty $Summary "drift"
  return [ordered]@{
    required_store_modules = @(Get-StringArrayProperty $Summary "requiredStoreModules")
    source_module_count = [int]($Summary.sourceModuleCount)
    source_schema_version_count = [int]($Summary.sourceSchemaVersionCount)
    required_runtime_outputs = @(Get-StringArrayProperty $Summary "requiredRuntimeOutputs")
    runtime_check_count = [int]($Summary.runtimeCheckCount)
    runtime_schema_version_count = [int]($Summary.runtimeSchemaVersionCount)
    drift = [ordered]@{
      missing_source_schema_version_modules = @(Get-StringArrayProperty $drift "missingSourceSchemaVersionModules")
      missing_runtime_schema_version_outputs = @(Get-StringArrayProperty $drift "missingRuntimeSchemaVersionOutputs")
      missing_required_runtime_outputs = @(Get-StringArrayProperty $drift "missingRequiredRuntimeOutputs")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-GovernanceStoreSchemaVersionReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "governance store schema version checks failed" }
    return "governance store schema version checks passed"
  }

  $details = New-GovernanceStoreSchemaVersionDetails $Summary
  $base = "governance store schema version checks {0} (source={1}/{2}; runtime={3}/{4})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.source_schema_version_count,
    [int]$details.source_module_count,
    [int]$details.runtime_schema_version_count,
    [int]$details.runtime_check_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  foreach ($entry in @(
    @{ name = "missing_source_schema_version_modules"; label = "source modules missing schema_version" },
    @{ name = "missing_runtime_schema_version_outputs"; label = "runtime outputs missing schema_version" },
    @{ name = "missing_required_runtime_outputs"; label = "required runtime outputs missing" }
  )) {
    $values = @(Get-StringArrayProperty $details.drift $entry.name)
    if ($values.Count -gt 0) {
      $fragments += "$($entry.label): $($values -join ', ')"
    }
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-GovernanceStoreSchemaVersionReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  foreach ($entry in @(
    @{ name = "missing_source_schema_version_modules"; label = "source modules missing schema_version" },
    @{ name = "missing_runtime_schema_version_outputs"; label = "runtime outputs missing schema_version" },
    @{ name = "missing_required_runtime_outputs"; label = "required runtime outputs missing" }
  )) {
    $values = @(Get-StringArrayProperty (Get-OptionalObjectProperty $details "drift") $entry.name)
    if ($values.Count -gt 0) {
      $issues += "governance_store_schema_versions $($entry.label): $($values -join ', ')"
    }
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-LocalWorkflowStoreSchemaVersionDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $drift = Get-OptionalObjectProperty $Summary "drift"
  $legacyReads = Get-OptionalObjectProperty $Summary "legacyReads"
  return [ordered]@{
    required_store_modules = @(Get-StringArrayProperty $Summary "requiredStoreModules")
    source_module_count = [int]($Summary.sourceModuleCount)
    source_schema_version_count = [int]($Summary.sourceSchemaVersionCount)
    required_runtime_outputs = @(Get-StringArrayProperty $Summary "requiredRuntimeOutputs")
    runtime_check_count = [int]($Summary.runtimeCheckCount)
    runtime_schema_version_count = [int]($Summary.runtimeSchemaVersionCount)
    legacy_reads = [ordered]@{
      workflow_task_queue = (Get-OptionalBooleanProperty $legacyReads "workflow_task_queue")
      workflow_queue_control = (Get-OptionalBooleanProperty $legacyReads "workflow_queue_control")
      template_marketplace = (Get-OptionalBooleanProperty $legacyReads "template_marketplace")
      workflow_node_cache = (Get-OptionalBooleanProperty $legacyReads "workflow_node_cache")
    }
    drift = [ordered]@{
      missing_source_schema_version_modules = @(Get-StringArrayProperty $drift "missingSourceSchemaVersionModules")
      missing_runtime_schema_version_outputs = @(Get-StringArrayProperty $drift "missingRuntimeSchemaVersionOutputs")
      missing_required_runtime_outputs = @(Get-StringArrayProperty $drift "missingRequiredRuntimeOutputs")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-LocalWorkflowStoreSchemaVersionReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "local workflow store schema version checks failed" }
    return "local workflow store schema version checks passed"
  }

  $details = New-LocalWorkflowStoreSchemaVersionDetails $Summary
  $base = "local workflow store schema version checks {0} (source={1}/{2}; runtime={3}/{4}; legacy_queue={5}; legacy_control={6}; legacy_templates={7}; legacy_cache={8})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.source_schema_version_count,
    [int]$details.source_module_count,
    [int]$details.runtime_schema_version_count,
    [int]$details.runtime_check_count,
    [string]$details.legacy_reads.workflow_task_queue,
    [string]$details.legacy_reads.workflow_queue_control,
    [string]$details.legacy_reads.template_marketplace,
    [string]$details.legacy_reads.workflow_node_cache

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  foreach ($entry in @(
    @{ name = "missing_source_schema_version_modules"; label = "source modules missing schema_version" },
    @{ name = "missing_runtime_schema_version_outputs"; label = "runtime outputs missing schema_version" },
    @{ name = "missing_required_runtime_outputs"; label = "required runtime outputs missing" }
  )) {
    $values = @(Get-StringArrayProperty $details.drift $entry.name)
    if ($values.Count -gt 0) {
      $fragments += "$($entry.label): $($values -join ', ')"
    }
  }
  foreach ($entry in @(
    @{ ok = [bool]$details.legacy_reads.workflow_task_queue; label = "legacy workflow queue migration missing" },
    @{ ok = [bool]$details.legacy_reads.workflow_queue_control; label = "legacy queue control migration missing" },
    @{ ok = [bool]$details.legacy_reads.template_marketplace; label = "legacy template marketplace migration missing" },
    @{ ok = [bool]$details.legacy_reads.workflow_node_cache; label = "legacy node cache migration missing" }
  )) {
    if (-not $entry.ok) {
      $fragments += [string]$entry.label
    }
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-LocalWorkflowStoreSchemaVersionReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  foreach ($entry in @(
    @{ name = "missing_source_schema_version_modules"; label = "source modules missing schema_version" },
    @{ name = "missing_runtime_schema_version_outputs"; label = "runtime outputs missing schema_version" },
    @{ name = "missing_required_runtime_outputs"; label = "required runtime outputs missing" }
  )) {
    $values = @(Get-StringArrayProperty (Get-OptionalObjectProperty $details "drift") $entry.name)
    if ($values.Count -gt 0) {
      $issues += "local_workflow_store_schema_versions $($entry.label): $($values -join ', ')"
    }
  }
  $legacyReads = Get-OptionalObjectProperty $details "legacy_reads"
  foreach ($entry in @(
    @{ name = "workflow_task_queue"; label = "legacy workflow queue migration missing" },
    @{ name = "workflow_queue_control"; label = "legacy queue control migration missing" },
    @{ name = "template_marketplace"; label = "legacy template marketplace migration missing" },
    @{ name = "workflow_node_cache"; label = "legacy node cache migration missing" }
  )) {
    if (-not (Get-OptionalBooleanProperty $legacyReads $entry.name)) {
      $issues += "local_workflow_store_schema_versions $($entry.label)"
    }
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-TemplatePackContractDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  return [ordered]@{
    schema_path = Get-OptionalPropertyValue $Summary "schemaPath"
    artifact_schema_version = Get-OptionalPropertyValue $Summary "artifactSchemaVersion"
    marketplace_entry_schema_version = Get-OptionalPropertyValue $Summary "marketplaceEntrySchemaVersion"
    import_migrated = (Get-OptionalBooleanProperty $Summary "importMigrated")
    install_migrated = (Get-OptionalBooleanProperty $Summary "installMigrated")
    exported_artifact_schema_version = Get-OptionalPropertyValue $Summary "exportedArtifactSchemaVersion"
    template_count = [int]($Summary.templateCount)
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-TemplatePackContractReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "template pack contract sync checks failed" }
    return "template pack contract sync checks passed"
  }

  $details = New-TemplatePackContractDetails $Summary
  $base = "template pack contract sync checks {0} (artifact={1}; entry={2}; import_migrated={3}; install_migrated={4}; exported_artifact={5}; templates={6})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [string]$details.artifact_schema_version,
    [string]$details.marketplace_entry_schema_version,
    [string]$details.import_migrated,
    [string]$details.install_migrated,
    [string]$details.exported_artifact_schema_version,
    [int]$details.template_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  if ([string]::IsNullOrWhiteSpace([string]$details.artifact_schema_version)) {
    $fragments += "artifact schema version missing"
  }
  if ([string]::IsNullOrWhiteSpace([string]$details.marketplace_entry_schema_version)) {
    $fragments += "marketplace entry schema version missing"
  }
  if (-not [bool]$details.import_migrated) {
    $fragments += "legacy template pack import migration missing"
  }
  if (-not [bool]$details.install_migrated) {
    $fragments += "template pack install migration missing"
  }
  if ([string]$details.exported_artifact_schema_version -ne [string]$details.artifact_schema_version) {
    $fragments += "template pack export schema drift"
  }
  if ([int]$details.template_count -lt 1) {
    $fragments += "template pack export has no templates"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-TemplatePackContractReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "artifact_schema_version"))) {
    $issues += "template_pack_contract_sync artifact schema version missing"
  }
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "marketplace_entry_schema_version"))) {
    $issues += "template_pack_contract_sync marketplace entry schema version missing"
  }
  if (-not (Get-OptionalBooleanProperty $details "import_migrated")) {
    $issues += "template_pack_contract_sync legacy template pack import migration missing"
  }
  if (-not (Get-OptionalBooleanProperty $details "install_migrated")) {
    $issues += "template_pack_contract_sync template pack install migration missing"
  }
  if ((Get-OptionalPropertyValue $details "artifact_schema_version") -ne (Get-OptionalPropertyValue $details "exported_artifact_schema_version")) {
    $issues += "template_pack_contract_sync exported artifact schema drift"
  }
  if ([int](Get-OptionalPropertyValue $details "template_count") -lt 1) {
    $issues += "template_pack_contract_sync exported template pack is empty"
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-LocalTemplateStorageContractDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  return [ordered]@{
    schema_path = Get-OptionalPropertyValue $Summary "schemaPath"
    storage_schema_version = Get-OptionalPropertyValue $Summary "storageSchemaVersion"
    entry_schema_version = Get-OptionalPropertyValue $Summary "entrySchemaVersion"
    legacy_storage_migrated = (Get-OptionalBooleanProperty $Summary "legacyStorageMigrated")
    local_storage_normalized_on_load = (Get-OptionalBooleanProperty $Summary "localStorageNormalizedOnLoad")
    local_save_versioned = (Get-OptionalBooleanProperty $Summary "localSaveVersioned")
    saved_entry_count = [int]($Summary.savedEntryCount)
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-LocalTemplateStorageContractReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "local template storage contract sync checks failed" }
    return "local template storage contract sync checks passed"
  }

  $details = New-LocalTemplateStorageContractDetails $Summary
  $base = "local template storage contract sync checks {0} (storage={1}; entry={2}; legacy_migrated={3}; normalized_on_load={4}; save_versioned={5}; saved_entries={6})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [string]$details.storage_schema_version,
    [string]$details.entry_schema_version,
    [string]$details.legacy_storage_migrated,
    [string]$details.local_storage_normalized_on_load,
    [string]$details.local_save_versioned,
    [int]$details.saved_entry_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  if ([string]::IsNullOrWhiteSpace([string]$details.storage_schema_version)) {
    $fragments += "local template storage schema version missing"
  }
  if ([string]::IsNullOrWhiteSpace([string]$details.entry_schema_version)) {
    $fragments += "local template entry schema version missing"
  }
  if (-not [bool]$details.legacy_storage_migrated) {
    $fragments += "legacy local template storage migration missing"
  }
  if (-not [bool]$details.local_storage_normalized_on_load) {
    $fragments += "local template storage no longer rewrites legacy payloads on load"
  }
  if (-not [bool]$details.local_save_versioned) {
    $fragments += "saveCurrentAsTemplate no longer writes versioned storage"
  }
  if ([int]$details.saved_entry_count -lt 1) {
    $fragments += "no local template entries persisted"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-LocalTemplateStorageContractReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "storage_schema_version"))) {
    $issues += "local_template_storage_contract_sync storage schema version missing"
  }
  if ([string]::IsNullOrWhiteSpace((Get-OptionalPropertyValue $details "entry_schema_version"))) {
    $issues += "local_template_storage_contract_sync entry schema version missing"
  }
  if (-not (Get-OptionalBooleanProperty $details "legacy_storage_migrated")) {
    $issues += "local_template_storage_contract_sync legacy storage migration missing"
  }
  if (-not (Get-OptionalBooleanProperty $details "local_storage_normalized_on_load")) {
    $issues += "local_template_storage_contract_sync legacy localStorage normalization missing"
  }
  if (-not (Get-OptionalBooleanProperty $details "local_save_versioned")) {
    $issues += "local_template_storage_contract_sync saveCurrentAsTemplate no longer writes versioned storage"
  }
  if ([int](Get-OptionalPropertyValue $details "saved_entry_count") -lt 1) {
    $issues += "local_template_storage_contract_sync no local template entries persisted"
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-OfflineTemplateCatalogDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $schemaPaths = Get-OptionalObjectProperty $Summary "schemaPaths"
  $filePaths = Get-OptionalObjectProperty $Summary "filePaths"
  $schemaVersions = Get-OptionalObjectProperty $Summary "schemaVersions"
  $migratedLegacy = Get-OptionalObjectProperty $Summary "migratedLegacy"
  $runtime = Get-OptionalObjectProperty $Summary "runtime"
  return [ordered]@{
    schema_paths = [ordered]@{
      theme = Get-OptionalPropertyValue $schemaPaths "theme"
      layout = Get-OptionalPropertyValue $schemaPaths "layout"
      registry = Get-OptionalPropertyValue $schemaPaths "registry"
    }
    file_paths = [ordered]@{
      theme = Get-OptionalPropertyValue $filePaths "theme"
      layout = Get-OptionalPropertyValue $filePaths "layout"
      registry = Get-OptionalPropertyValue $filePaths "registry"
    }
    schema_versions = [ordered]@{
      theme = Get-OptionalPropertyValue $schemaVersions "theme"
      layout = Get-OptionalPropertyValue $schemaVersions "layout"
      registry = Get-OptionalPropertyValue $schemaVersions "registry"
    }
    migrated_legacy = [ordered]@{
      theme = (Get-OptionalBooleanProperty $migratedLegacy "theme")
      layout = (Get-OptionalBooleanProperty $migratedLegacy "layout")
      registry = (Get-OptionalBooleanProperty $migratedLegacy "registry")
    }
    runtime = [ordered]@{
      theme_title = Get-OptionalPropertyValue $runtime "themeTitle"
      layout_rows = [int](Get-OptionalPropertyValue $runtime "layoutRows")
      cleaning_template_count = [int](Get-OptionalPropertyValue $runtime "cleaningTemplateCount")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-OfflineTemplateCatalogReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "offline template catalog sync checks failed" }
    return "offline template catalog sync checks passed"
  }

  $details = New-OfflineTemplateCatalogDetails $Summary
  $base = "offline template catalog sync checks {0} (theme={1}; layout={2}; registry={3}; runtime_theme={4}; runtime_layout_rows={5}; runtime_templates={6})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [string]$details.schema_versions.theme,
    [string]$details.schema_versions.layout,
    [string]$details.schema_versions.registry,
    [string]$details.runtime.theme_title,
    [int]$details.runtime.layout_rows,
    [int]$details.runtime.cleaning_template_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  foreach ($entry in @(
    @{ name = "theme"; label = "legacy office theme migration missing" },
    @{ name = "layout"; label = "legacy office layout migration missing" },
    @{ name = "registry"; label = "legacy cleaning template registry migration missing" }
  )) {
    if (-not (Get-OptionalBooleanProperty $details.migrated_legacy $entry.name)) {
      $fragments += [string]$entry.label
    }
  }
  if ([int]$details.runtime.cleaning_template_count -lt 1) {
    $fragments += "offline engine cleaning template registry is empty"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-OfflineTemplateCatalogReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  foreach ($entry in @(
    @{ name = "theme"; label = "legacy office theme migration missing" },
    @{ name = "layout"; label = "legacy office layout migration missing" },
    @{ name = "registry"; label = "legacy cleaning template registry migration missing" }
  )) {
    if (-not (Get-OptionalBooleanProperty (Get-OptionalObjectProperty $details "migrated_legacy") $entry.name)) {
      $issues += "offline_template_catalog_sync $($entry.label)"
    }
  }
  if ([int](Get-OptionalPropertyValue (Get-OptionalObjectProperty $details "runtime") "cleaning_template_count") -lt 1) {
    $issues += "offline_template_catalog_sync offline engine cleaning template registry is empty"
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function New-NodeConfigSchemaCoverageDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $qualityCounts = Get-OptionalObjectProperty $Summary "qualityCounts"
  $drift = Get-OptionalObjectProperty $Summary "drift"
  return [ordered]@{
    contract_path = Get-OptionalPropertyValue $Summary "contractPath"
    rust_authority_path = Get-OptionalPropertyValue $Summary "rustAuthorityPath"
    rust_handler_path = Get-OptionalPropertyValue $Summary "rustHandlerPath"
    glue_validation_client_path = Get-OptionalPropertyValue $Summary "glueValidationClientPath"
    contract_authority = [string]($Summary.contractAuthority)
    contract_schema_version = [string]($Summary.contractSchemaVersion)
    contract_validator_kind_count = [int]($Summary.contractValidatorKindCount)
    target_count = [int]($Summary.targetCount)
    covered_count = [int]($Summary.coveredCount)
    minimum_nested_shape_constrained = [int]($Summary.minimumNestedShapeConstrained)
    nested_shape_constrained_count = [int]($Summary.nestedShapeConstrainedCount)
    nested_shape_constrained_deficit = [int]($Summary.nestedShapeConstrainedDeficit)
    required_nested_node_types = @(Get-StringArrayProperty $Summary "requiredNestedNodeTypes")
    required_nested_satisfied = @(Get-StringArrayProperty $Summary "requiredNestedSatisfied")
    required_nested_missing = @(Get-StringArrayProperty $Summary "requiredNestedMissing")
    rust_authority_covered_count = [int](Get-OptionalPropertyValue $Summary "rustAuthorityCoveredCount")
    quality_counts = [ordered]@{
      typed = [int](Get-OptionalPropertyValue $qualityCounts "typed")
      enum_constrained = [int](Get-OptionalPropertyValue $qualityCounts "enum_constrained")
      nested_shape_constrained = [int](Get-OptionalPropertyValue $qualityCounts "nested_shape_constrained")
    }
    drift = [ordered]@{
      rust_authority_drift = @(Get-StringArrayProperty $drift "rustAuthorityDrift")
      rust_handler_drift = @(Get-StringArrayProperty $drift "rustHandlerDrift")
      glue_validation_client_drift = @(Get-StringArrayProperty $drift "glueValidationClientDrift")
      desktop_generated_helper_references = @(Get-StringArrayProperty $drift "desktopGeneratedHelperReferences")
      packaging_generated_helper_references = @(Get-StringArrayProperty $drift "packagingGeneratedHelperReferences")
      generated_desktop_helper_files_present = @(Get-StringArrayProperty $drift "generatedDesktopHelperFilesPresent")
      required_nested_types_missing_from_contract = @(Get-StringArrayProperty $drift "requiredNestedTypesMissingFromContract")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-NodeConfigSchemaCoverageReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "node config schema coverage checks failed" }
    return "node config schema coverage checks passed"
  }

  $details = New-NodeConfigSchemaCoverageDetails $Summary
  $typed = [int]($details.quality_counts.typed)
  $enumConstrained = [int]($details.quality_counts.enum_constrained)
  $nested = [int]($details.quality_counts.nested_shape_constrained)
  $rustAuthorityCovered = [int]($details.rust_authority_covered_count)
  $base = "node config schema coverage checks {0} ({1}/{2}; typed={3}; enum_constrained={4}; nested_shape_constrained={5}; rust_authority={6}/{2}; contract=node_config_contracts.v1.json)" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.covered_count,
    [int]$details.target_count,
    $typed,
    $enumConstrained,
    $nested,
    $rustAuthorityCovered

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  $requiredMissing = @($details.required_nested_missing)
  if ($requiredMissing.Count -gt 0) {
    $fragments += "required nested missing: $($requiredMissing -join ', ')"
  }
  $nestedMinimum = [int]$details.minimum_nested_shape_constrained
  if ($nestedMinimum -gt 0 -and [int]$details.nested_shape_constrained_count -lt $nestedMinimum) {
    $fragments += "nested_shape_constrained deficit: $([int]$details.nested_shape_constrained_count)/$nestedMinimum"
  }
  if ([int]$details.covered_count -lt [int]$details.target_count) {
    $fragments += "coverage gap: $([int]$details.covered_count)/$([int]$details.target_count)"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-NodeConfigReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  $requiredMissing = @(Get-StringArrayProperty $details "required_nested_missing")
  if ($requiredMissing.Count -gt 0) {
    $issues += "node_config_schema_coverage missing required nested node coverage: $($requiredMissing -join ', ')"
  }

  $nestedMinimum = [int](Get-OptionalPropertyValue $details "minimum_nested_shape_constrained")
  $nestedCount = [int](Get-OptionalPropertyValue $details "nested_shape_constrained_count")
  if ($nestedMinimum -gt 0 -and $nestedCount -lt $nestedMinimum) {
    $issues += "node_config_schema_coverage nested_shape_constrained deficit: $nestedCount/$nestedMinimum"
  }

  $targetCount = [int](Get-OptionalPropertyValue $details "target_count")
  $coveredCount = [int](Get-OptionalPropertyValue $details "covered_count")
  if ($targetCount -gt 0 -and $coveredCount -lt $targetCount) {
    $issues += "node_config_schema_coverage coverage gap: $coveredCount/$targetCount"
  }

  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "rust_authority_drift"; label = "rust authority drift" },
    @{ name = "rust_handler_drift"; label = "rust workflow endpoint drift" },
    @{ name = "glue_validation_client_drift"; label = "glue validation client drift" },
    @{ name = "desktop_generated_helper_references"; label = "desktop generated helper references still present" },
    @{ name = "packaging_generated_helper_references"; label = "packaging generated helper references still present" },
    @{ name = "generated_desktop_helper_files_present"; label = "generated desktop helper files still present" },
    @{ name = "required_nested_types_missing_from_contract"; label = "required nested contract types missing" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $issues += "node_config_schema_coverage $($entry.label): $($values -join ', ')"
    }
  }

  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }

  return @($issues | Sort-Object -Unique)
}
function New-LocalNodeCatalogPolicyDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $drift = Get-OptionalObjectProperty $Summary "drift"
  return [ordered]@{
    local_node_type_count = [int]($Summary.localNodeTypeCount)
    local_catalog_count = [int]($Summary.localCatalogCount)
    required_local_node_types = @(Get-StringArrayProperty $Summary "requiredLocalNodeTypes")
    drift = [ordered]@{
      invalid_sections = @(Get-StringArrayProperty $drift "invalidSections")
      missing_section_types = @(Get-StringArrayProperty $drift "missingSectionTypes")
      missing_presentation_types = @(Get-StringArrayProperty $drift "missingPresentationTypes")
      stale_presentation_types = @(Get-StringArrayProperty $drift "stalePresentationTypes")
      invalid_presentation_entries = @(Get-StringArrayProperty $drift "invalidPresentationEntries")
      duplicate_pinned_types = @(Get-StringArrayProperty $drift "duplicatePinnedTypes")
      stale_pinned_types = @(Get-StringArrayProperty $drift "stalePinnedTypes")
      missing_catalog_types = @(Get-StringArrayProperty $drift "missingCatalogTypes")
      stale_catalog_types = @(Get-StringArrayProperty $drift "staleCatalogTypes")
      missing_catalog_policy_source_types = @(Get-StringArrayProperty $drift "missingCatalogPolicySourceTypes")
      catalog_metadata_drift = @(Get-StringArrayProperty $drift "catalogMetadataDrift")
      required_local_type_missing = @(Get-StringArrayProperty $drift "requiredLocalTypeMissing")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-LocalNodeCatalogPolicyReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "local node catalog policy checks failed" }
    return "local node catalog policy checks passed"
  }

  $details = New-LocalNodeCatalogPolicyDetails $Summary
  $base = "local node catalog policy checks {0} (policy={1}; catalog={2})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.local_node_type_count,
    [int]$details.local_catalog_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "invalid_sections"; label = "invalid sections" },
    @{ name = "missing_section_types"; label = "missing section types" },
    @{ name = "missing_presentation_types"; label = "missing presentations" },
    @{ name = "stale_presentation_types"; label = "stale presentations" },
    @{ name = "invalid_presentation_entries"; label = "invalid presentations" },
    @{ name = "duplicate_pinned_types"; label = "duplicate pinned types" },
    @{ name = "stale_pinned_types"; label = "stale pinned types" },
    @{ name = "missing_catalog_types"; label = "catalog missing types" },
    @{ name = "stale_catalog_types"; label = "catalog stale types" },
    @{ name = "missing_catalog_policy_source_types"; label = "catalog policy source drift" },
    @{ name = "catalog_metadata_drift"; label = "catalog metadata drift" },
    @{ name = "required_local_type_missing"; label = "required local types missing" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $fragments += "$($entry.label): $($values -join ', ')"
    }
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-LocalNodeCatalogReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "invalid_sections"; label = "local node palette sections invalid" },
    @{ name = "missing_section_types"; label = "local node palette policy missing node types" },
    @{ name = "missing_presentation_types"; label = "local node presentations missing node types" },
    @{ name = "stale_presentation_types"; label = "local node presentations have stale node types" },
    @{ name = "invalid_presentation_entries"; label = "local node presentations have invalid entries" },
    @{ name = "duplicate_pinned_types"; label = "local node palette pinned order duplicated" },
    @{ name = "stale_pinned_types"; label = "local node palette pinned order has stale node types" },
    @{ name = "missing_catalog_types"; label = "defaults catalog missing local node types" },
    @{ name = "stale_catalog_types"; label = "defaults catalog has stale local node types" },
    @{ name = "missing_catalog_policy_source_types"; label = "defaults catalog local node policy_source drift" },
    @{ name = "catalog_metadata_drift"; label = "defaults catalog local node metadata drift" },
    @{ name = "required_local_type_missing"; label = "required local node types missing from policy truth" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $issues += "local_node_catalog_policy $($entry.label): $($values -join ', ')"
    }
  }

  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }

  return @($issues | Sort-Object -Unique)
}
function New-OperatorCatalogSyncDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $drift = Get-OptionalObjectProperty $Summary "drift"
  return [ordered]@{
    manifest_path = Get-OptionalPropertyValue $Summary "manifestPath"
    schema_path = Get-OptionalPropertyValue $Summary "schemaPath"
    desktop_module_path = Get-OptionalPropertyValue $Summary "desktopModulePath"
    renderer_module_path = Get-OptionalPropertyValue $Summary "rendererModulePath"
    manifest_operator_count = [int]($Summary.manifestOperatorCount)
    published_count = [int]($Summary.publishedCount)
    workflow_count = [int]($Summary.workflowCount)
    desktop_exposable_count = [int]($Summary.desktopExposableCount)
    desktop_module_count = [int]($Summary.desktopModuleCount)
    rust_mapped_count = [int]($Summary.rustMappedCount)
    defaults_catalog_count = [int]($Summary.defaultsCatalogCount)
    builtin_operator_count = [int]($Summary.builtinOperatorCount)
    required_published_operators = @(Get-StringArrayProperty $Summary "requiredPublishedOperators")
    drift = [ordered]@{
      manifest_missing_operators = @(Get-StringArrayProperty $drift "manifestMissingOperators")
      manifest_stale_operators = @(Get-StringArrayProperty $drift "manifestStaleOperators")
      manifest_metadata_drift = @(Get-StringArrayProperty $drift "manifestMetadataDrift")
      desktop_module_missing_operators = @(Get-StringArrayProperty $drift "desktopModuleMissingOperators")
      desktop_module_stale_operators = @(Get-StringArrayProperty $drift "desktopModuleStaleOperators")
      desktop_module_metadata_drift = @(Get-StringArrayProperty $drift "desktopModuleMetadataDrift")
      renderer_module_drift = @(Get-StringArrayProperty $drift "rendererModuleDrift")
      invalid_palette_sections = @(Get-StringArrayProperty $drift "invalidPaletteSections")
      missing_palette_section_domains = @(Get-StringArrayProperty $drift "missingPaletteSectionDomains")
      stale_pinned_operators = @(Get-StringArrayProperty $drift "stalePinnedOperators")
      duplicate_pinned_operators = @(Get-StringArrayProperty $drift "duplicatePinnedOperators")
      missing_presentation_operators = @(Get-StringArrayProperty $drift "missingPresentationOperators")
      stale_presentation_operators = @(Get-StringArrayProperty $drift "stalePresentationOperators")
      invalid_presentation_entries = @(Get-StringArrayProperty $drift "invalidPresentationEntries")
      missing_published_in_catalog = @(Get-StringArrayProperty $drift "missingPublishedInCatalog")
      missing_published_in_routing = @(Get-StringArrayProperty $drift "missingPublishedInRouting")
      missing_desktop_exposable_in_catalog = @(Get-StringArrayProperty $drift "missingDesktopExposableInCatalog")
      missing_desktop_exposable_in_routing = @(Get-StringArrayProperty $drift "missingDesktopExposableInRouting")
      missing_catalog_group_entries = @(Get-StringArrayProperty $drift "missingCatalogGroupEntries")
      missing_catalog_policy_section_entries = @(Get-StringArrayProperty $drift "missingCatalogPolicySectionEntries")
      missing_catalog_policy_source_entries = @(Get-StringArrayProperty $drift "missingCatalogPolicySourceEntries")
      stale_rust_routing = @(Get-StringArrayProperty $drift "staleRustRouting")
      stale_catalog_operators = @(Get-StringArrayProperty $drift "staleCatalogOperators")
      required_published_missing = @(Get-StringArrayProperty $drift "requiredPublishedMissing")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-OperatorCatalogSyncReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "operator catalog sync checks failed" }
    return "operator catalog sync checks passed"
  }

  $details = New-OperatorCatalogSyncDetails $Summary
  $base = "operator catalog sync checks {0} (published={1}; workflow={2}; rust_mapped={3}; defaults_catalog={4}; builtin={5})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [int]$details.published_count,
    [int]$details.workflow_count,
    [int]$details.rust_mapped_count,
    [int]$details.defaults_catalog_count,
    [int]$details.builtin_operator_count

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "manifest_missing_operators"; label = "checked-in manifest missing" },
    @{ name = "manifest_stale_operators"; label = "checked-in manifest stale" },
    @{ name = "manifest_metadata_drift"; label = "checked-in manifest metadata drift" },
    @{ name = "desktop_module_missing_operators"; label = "desktop manifest module missing" },
    @{ name = "desktop_module_stale_operators"; label = "desktop manifest module stale" },
    @{ name = "desktop_module_metadata_drift"; label = "desktop manifest module metadata drift" },
    @{ name = "renderer_module_drift"; label = "renderer manifest module drift" },
    @{ name = "invalid_palette_sections"; label = "rust palette sections invalid" },
    @{ name = "missing_palette_section_domains"; label = "rust palette policy missing domains" },
    @{ name = "stale_pinned_operators"; label = "rust palette pinned order stale" },
    @{ name = "duplicate_pinned_operators"; label = "rust palette pinned order duplicated" },
    @{ name = "missing_presentation_operators"; label = "rust presentation coverage missing" },
    @{ name = "stale_presentation_operators"; label = "rust presentation stale" },
    @{ name = "invalid_presentation_entries"; label = "rust presentation invalid" },
    @{ name = "missing_published_in_catalog"; label = "defaults catalog missing" },
    @{ name = "missing_published_in_routing"; label = "desktop rust routing missing" },
    @{ name = "missing_desktop_exposable_in_catalog"; label = "desktop catalog missing desktop-exposable operators" },
    @{ name = "missing_desktop_exposable_in_routing"; label = "desktop rust routing missing desktop-exposable operators" },
    @{ name = "missing_catalog_group_entries"; label = "desktop catalog missing rust groups" },
    @{ name = "missing_catalog_policy_section_entries"; label = "desktop catalog missing rust policy sections" },
    @{ name = "missing_catalog_policy_source_entries"; label = "desktop catalog missing rust policy sources" },
    @{ name = "stale_rust_routing"; label = "stale rust routing" },
    @{ name = "stale_catalog_operators"; label = "stale defaults catalog operators" },
    @{ name = "required_published_missing"; label = "required published operators missing" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $fragments += "$($entry.label): $($values -join ', ')"
    }
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-OperatorCatalogReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  $drift = Get-OptionalObjectProperty $details "drift"
  foreach ($entry in @(
    @{ name = "manifest_missing_operators"; label = "checked-in manifest missing Rust operators" },
    @{ name = "manifest_stale_operators"; label = "checked-in manifest has stale operators" },
    @{ name = "manifest_metadata_drift"; label = "checked-in manifest metadata drift" },
    @{ name = "desktop_module_missing_operators"; label = "desktop rust operator manifest module missing desktop-exposable operators" },
    @{ name = "desktop_module_stale_operators"; label = "desktop rust operator manifest module has stale operators" },
    @{ name = "desktop_module_metadata_drift"; label = "desktop rust operator manifest module metadata drift" },
    @{ name = "renderer_module_drift"; label = "renderer rust operator manifest module drift" },
    @{ name = "invalid_palette_sections"; label = "rust operator palette sections invalid" },
    @{ name = "missing_palette_section_domains"; label = "rust operator palette policy missing domains" },
    @{ name = "stale_pinned_operators"; label = "rust operator palette pinned order has stale operators" },
    @{ name = "duplicate_pinned_operators"; label = "rust operator palette pinned order has duplicate operators" },
    @{ name = "missing_presentation_operators"; label = "rust operator presentations missing desktop-exposable operators" },
    @{ name = "stale_presentation_operators"; label = "rust operator presentations have stale operators" },
    @{ name = "invalid_presentation_entries"; label = "rust operator presentations have invalid entries" },
    @{ name = "missing_published_in_catalog"; label = "defaults catalog missing published Rust operators" },
    @{ name = "missing_published_in_routing"; label = "desktop rust routing missing published Rust operators" },
    @{ name = "missing_desktop_exposable_in_catalog"; label = "desktop defaults catalog missing desktop-exposable Rust operators" },
    @{ name = "missing_desktop_exposable_in_routing"; label = "desktop rust routing missing desktop-exposable Rust operators" },
    @{ name = "missing_catalog_group_entries"; label = "desktop defaults catalog missing Rust operator groups" },
    @{ name = "missing_catalog_policy_section_entries"; label = "desktop defaults catalog missing Rust operator policy sections" },
    @{ name = "missing_catalog_policy_source_entries"; label = "desktop defaults catalog missing Rust operator policy sources" },
    @{ name = "stale_rust_routing"; label = "desktop rust routing exposes operators outside manifest desktop exposure" },
    @{ name = "stale_catalog_operators"; label = "desktop defaults catalog exposes Rust operators outside manifest desktop exposure" },
    @{ name = "required_published_missing"; label = "required published Rust operators missing from catalog truth" }
  )) {
    $values = @(Get-StringArrayProperty $drift $entry.name)
    if ($values.Count -gt 0) {
      $issues += "operator_catalog_sync $($entry.label): $($values -join ', ')"
    }
  }

  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }

  return @($issues | Sort-Object -Unique)
}
function New-CleaningRustV2RolloutDetails($Summary) {
  if ($null -eq $Summary) { return $null }
  $shadow = Get-OptionalObjectProperty $Summary "latest_shadow_compare_summary"
  $runModeAudit = Get-OptionalObjectProperty $Summary "run_mode_audit"
  return [ordered]@{
    mode = Get-OptionalPropertyValue $Summary "mode"
    verify_on_default = (Get-OptionalBooleanProperty $Summary "verify_on_default")
    run_mode_audit_path = Get-OptionalPropertyValue $Summary "run_mode_audit_path"
    sidecar_consistency_report_path = Get-OptionalPropertyValue $Summary "sidecar_consistency_report_path"
    reason_counts_mismatch_scenarios = @(Get-StringArrayProperty $Summary "reason_counts_mismatch_scenarios")
    skipped = @(Get-StringArrayProperty $Summary "skipped")
    latest_shadow_compare_summary = [ordered]@{
      status = Get-OptionalPropertyValue $shadow "status"
      matched = (Get-OptionalBooleanProperty $shadow "matched")
      mismatch_count = [int](Get-OptionalPropertyValue $shadow "mismatch_count")
      compare_fields = @(Get-StringArrayProperty $shadow "compare_fields")
    }
    run_mode_audit = [ordered]@{
      required_fields = @(Get-StringArrayProperty $runModeAudit "required_fields")
      missing_fields = @(Get-StringArrayProperty $runModeAudit "missing_fields")
    }
    issues = @(Get-StringArrayProperty $Summary "issues")
  }
}
function Get-CleaningRustV2RolloutReason($Summary, [string]$Status) {
  if ($null -eq $Summary) {
    if ($Status -eq "failed") { return "cleaning rust v2 rollout checks failed" }
    return "cleaning rust v2 rollout checks passed"
  }

  $details = New-CleaningRustV2RolloutDetails $Summary
  $shadow = Get-OptionalObjectProperty $details "latest_shadow_compare_summary"
  $base = "cleaning rust v2 rollout checks {0} (mode={1}; verify_on_default={2}; shadow_compare={3}; reason_count_mismatches={4}; skipped={5})" -f `
    $(if ($Status -eq "failed") { "failed" } else { "passed" }),
    [string]$details.mode,
    [string]$details.verify_on_default,
    [string](Get-OptionalPropertyValue $shadow "status"),
    [int](@($details.reason_counts_mismatch_scenarios).Count),
    [int](@($details.skipped).Count)

  if ($Status -ne "failed") {
    return $base
  }

  $fragments = @()
  $missingAuditFields = @(Get-StringArrayProperty (Get-OptionalObjectProperty $details "run_mode_audit") "missing_fields")
  if ($missingAuditFields.Count -gt 0) {
    $fragments += "run_mode_audit missing fields: $($missingAuditFields -join ', ')"
  }
  $reasonMismatches = @($details.reason_counts_mismatch_scenarios)
  if ($reasonMismatches.Count -gt 0) {
    $fragments += "reason_counts mismatches: $($reasonMismatches -join ', ')"
  }
  if ($fragments.Count -eq 0 -and @($details.issues).Count -gt 0) {
    $fragments += [string]$details.issues[0]
  }
  if ($fragments.Count -eq 0) {
    return $base
  }
  return "$base; $($fragments -join '; ')"
}
function Get-CleaningRustV2RolloutReleaseReadyIssues($Item) {
  $details = Get-OptionalObjectProperty $Item "details"
  if ($null -eq $details) { return @() }

  $issues = @()
  $missingAuditFields = @(Get-StringArrayProperty (Get-OptionalObjectProperty $details "run_mode_audit") "missing_fields")
  if ($missingAuditFields.Count -gt 0) {
    $issues += "cleaning_rust_v2_rollout run_mode_audit missing fields: $($missingAuditFields -join ', ')"
  }
  $reasonMismatches = @(Get-StringArrayProperty $details "reason_counts_mismatch_scenarios")
  if ($reasonMismatches.Count -gt 0) {
    $issues += "cleaning_rust_v2_rollout sidecar consistency reason_counts mismatch: $($reasonMismatches -join ', ')"
  }
  if ($issues.Count -eq 0) {
    $issues += @(Get-StringArrayProperty $details "issues")
  }
  return @($issues | Sort-Object -Unique)
}
function Get-ReleaseReadyBoundaryIssues([string]$Name, $Item) {
  if ($Name -eq "workflow_contract_sync") {
    return @(Get-WorkflowContractReleaseReadyIssues $Item)
  }
  if ($Name -eq "governance_control_plane_boundary") {
    return @(Get-GovernanceControlPlaneBoundaryReleaseReadyIssues $Item)
  }
  if ($Name -eq "governance_store_schema_versions") {
    return @(Get-GovernanceStoreSchemaVersionReleaseReadyIssues $Item)
  }
  if ($Name -eq "local_workflow_store_schema_versions") {
    return @(Get-LocalWorkflowStoreSchemaVersionReleaseReadyIssues $Item)
  }
  if ($Name -eq "template_pack_contract_sync") {
    return @(Get-TemplatePackContractReleaseReadyIssues $Item)
  }
  if ($Name -eq "local_template_storage_contract_sync") {
    return @(Get-LocalTemplateStorageContractReleaseReadyIssues $Item)
  }
  if ($Name -eq "offline_template_catalog_sync") {
    return @(Get-OfflineTemplateCatalogReleaseReadyIssues $Item)
  }
  if ($Name -eq "node_config_schema_coverage") {
    return @(Get-NodeConfigReleaseReadyIssues $Item)
  }
  if ($Name -eq "local_node_catalog_policy") {
    return @(Get-LocalNodeCatalogReleaseReadyIssues $Item)
  }
  if ($Name -eq "operator_catalog_sync") {
    return @(Get-OperatorCatalogReleaseReadyIssues $Item)
  }
  if ($Name -eq "cleaning_rust_v2_rollout") {
    return @(Get-CleaningRustV2RolloutReleaseReadyIssues $Item)
  }
  return @()
}
function Get-ScorecardBoundary($Payload, [string]$Name) {
  $boundaries = Get-OptionalObjectProperty $Payload "boundaries"
  return Get-OptionalObjectProperty $boundaries $Name
}
function Get-ScorecardFrontendItem($Payload, [string]$Name) {
  $frontend = Get-OptionalObjectProperty $Payload "frontend"
  return Get-OptionalObjectProperty $frontend $Name
}
function New-MergedBoundarySummary([string]$Name, $Item, [string]$Profile, [string]$GeneratedAt, [int]$StaleAfterHours) {
  $ts = Parse-ScorecardTimestamp $GeneratedAt
  $ageHours = if ($ts) { [math]::Round(((Get-Date) - $ts).TotalHours, 3) } else { $null }
  $fresh = if ($null -eq $ageHours) { $false } else { $ageHours -le $StaleAfterHours }
  return [ordered]@{
    name = $Name
    status = Get-OptionalPropertyValue $Item "status"
    reason = Get-OptionalPropertyValue $Item "reason"
    source_profile = $Profile
    source_generated_at = $GeneratedAt
    age_hours = $ageHours
    stale_after_hours = $StaleAfterHours
    fresh = $fresh
    evidence_path = Get-OptionalPropertyValue $Item "evidence_path"
    details = Get-OptionalObjectProperty $Item "details"
  }
}
function Select-MergedBoundarySummary([string]$Name, [object[]]$ProfileCards, [scriptblock]$Selector, [int]$StaleAfterHours) {
  $candidates = @()
  foreach ($card in $ProfileCards) {
    $payload = $card.payload
    if ($null -eq $payload) { continue }
    $item = & $Selector $payload
    if ($null -eq $item) { continue }
    $status = [string]($item.status)
    if ([string]::IsNullOrWhiteSpace($status)) { continue }
    $generatedAt = Resolve-ScorecardItemGeneratedAt -Payload $payload -Item $item
    $sortTs = Parse-ScorecardTimestamp $generatedAt
    $candidates += [pscustomobject]@{
      profile = $card.profile
      item = $item
      generated_at = $generatedAt
      sort_ts = if ($sortTs) { $sortTs } else { [datetime]::MinValue }
    }
  }
  if ($candidates.Count -eq 0) {
    return [ordered]@{
      name = $Name
      status = "missing"
      reason = "no scorecard coverage available"
      source_profile = ""
      source_generated_at = ""
      age_hours = $null
      stale_after_hours = $StaleAfterHours
      fresh = $false
      evidence_path = ""
    }
  }
  $nonSkipped = @($candidates | Where-Object { [string]$_.item.status -notin @("skipped", "missing", "unreadable") } | Sort-Object sort_ts -Descending)
  $selected = if ($nonSkipped.Count -gt 0) { $nonSkipped[0] } else { (@($candidates | Sort-Object sort_ts -Descending))[0] }
  return New-MergedBoundarySummary -Name $Name -Item $selected.item -Profile ([string]$selected.profile) -GeneratedAt ([string]$selected.generated_at) -StaleAfterHours $StaleAfterHours
}
function Write-ArchitectureReleaseReadyScorecard([string]$Directory, [string]$Stamp) {
  $profileOrder = @("default", "quick", "full", "compatibility")
  $profileCards = foreach ($profile in $profileOrder) {
    $path = Join-Path $Directory ("architecture_scorecard_{0}_latest.json" -f $profile)
    $payload = Read-ArchitectureScorecardPayload $path
    [ordered]@{
      profile = $profile
      path = $path
      payload = $payload
    }
  }
  $staleAfterHours = 72
  $profiles = [ordered]@{}
  foreach ($entry in $profileCards) {
    $payload = $entry.payload
    $profiles[$entry.profile] = [ordered]@{
      path = $entry.path
      exists = ($null -ne $payload)
      generated_at = if ($payload) { [string]($payload.generated_at) } else { "" }
      overall_status = if ($payload) { [string]($payload.overall_status) } else { "missing" }
    }
  }

  $boundaries = [ordered]@{
    workflow_contract_sync = Select-MergedBoundarySummary -Name "workflow_contract_sync" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "workflow_contract_sync" } -StaleAfterHours $staleAfterHours
    governance_control_plane_boundary = Select-MergedBoundarySummary -Name "governance_control_plane_boundary" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "governance_control_plane_boundary" } -StaleAfterHours $staleAfterHours
    governance_store_schema_versions = Select-MergedBoundarySummary -Name "governance_store_schema_versions" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "governance_store_schema_versions" } -StaleAfterHours $staleAfterHours
    local_workflow_store_schema_versions = Select-MergedBoundarySummary -Name "local_workflow_store_schema_versions" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "local_workflow_store_schema_versions" } -StaleAfterHours $staleAfterHours
    template_pack_contract_sync = Select-MergedBoundarySummary -Name "template_pack_contract_sync" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "template_pack_contract_sync" } -StaleAfterHours $staleAfterHours
    local_template_storage_contract_sync = Select-MergedBoundarySummary -Name "local_template_storage_contract_sync" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "local_template_storage_contract_sync" } -StaleAfterHours $staleAfterHours
    offline_template_catalog_sync = Select-MergedBoundarySummary -Name "offline_template_catalog_sync" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "offline_template_catalog_sync" } -StaleAfterHours $staleAfterHours
    node_config_schema_coverage = Select-MergedBoundarySummary -Name "node_config_schema_coverage" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "node_config_schema_coverage" } -StaleAfterHours $staleAfterHours
    local_node_catalog_policy = Select-MergedBoundarySummary -Name "local_node_catalog_policy" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "local_node_catalog_policy" } -StaleAfterHours $staleAfterHours
    operator_catalog_sync = Select-MergedBoundarySummary -Name "operator_catalog_sync" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "operator_catalog_sync" } -StaleAfterHours $staleAfterHours
    cleaning_rust_v2_rollout = Select-MergedBoundarySummary -Name "cleaning_rust_v2_rollout" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "cleaning_rust_v2_rollout" } -StaleAfterHours $staleAfterHours
    frontend_convergence = Select-MergedBoundarySummary -Name "frontend_convergence" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardBoundary $payload "frontend_convergence" } -StaleAfterHours $staleAfterHours
    frontend_primary_verification = Select-MergedBoundarySummary -Name "frontend_primary_verification" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardFrontendItem $payload "primary" } -StaleAfterHours $staleAfterHours
    frontend_compatibility_verification = Select-MergedBoundarySummary -Name "frontend_compatibility_verification" -ProfileCards $profileCards -Selector { param($payload) Get-ScorecardFrontendItem $payload "compatibility" } -StaleAfterHours $staleAfterHours
  }

  $issues = @()
  foreach ($entry in $boundaries.GetEnumerator()) {
    $name = [string]$entry.Key
    $item = $entry.Value
    $status = [string]($item.status)
    $boundaryIssues = @(Get-ReleaseReadyBoundaryIssues -Name $name -Item $item)
    if ($status -eq "failed") {
      if ($boundaryIssues.Count -gt 0) {
        $issues += $boundaryIssues
      } else {
        $issues += "$name failed"
      }
    } elseif ($status -in @("missing", "skipped", "unreadable")) {
      $issues += "$name lacks release-ready coverage"
    } elseif ($boundaryIssues.Count -gt 0) {
      $issues += $boundaryIssues
    }
    if (-not $item.fresh) {
      $issues += "$name is stale or missing freshness data"
    }
  }

  $overall = if ($issues.Count -gt 0) { "incomplete" } else { "passed" }
  $payload = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    profile = "release_ready"
    source_script = "ops/scripts/ci_check.ps1"
    stale_after_hours = $staleAfterHours
    overall_status = $overall
    profiles = $profiles
    boundaries = $boundaries
    issues = @($issues | Sort-Object -Unique)
  }

  $jsonLatestPath = Join-Path $Directory "architecture_scorecard_release_ready_latest.json"
  $mdLatestPath = Join-Path $Directory "architecture_scorecard_release_ready_latest.md"
  $jsonSnapshotPath = Join-Path $Directory ("architecture_scorecard_release_ready_{0}.json" -f $Stamp)
  $mdSnapshotPath = Join-Path $Directory ("architecture_scorecard_release_ready_{0}.md" -f $Stamp)
  $json = ($payload | ConvertTo-Json -Depth 10)
  Set-Content -Path $jsonLatestPath -Value $json -Encoding UTF8
  Set-Content -Path $jsonSnapshotPath -Value $json -Encoding UTF8

  $lines = @()
  $lines += "# Architecture Scorecard (Release Ready)"
  $lines += ""
  $lines += "- generated_at: $($payload.generated_at)"
  $lines += "- overall_status: $($payload.overall_status)"
  $lines += "- stale_after_hours: $($payload.stale_after_hours)"
  $lines += ""
  $lines += "| Boundary | Status | Profile | Fresh | Reason |"
  $lines += "|---|---|---|---|---|"
  foreach ($entry in $payload.boundaries.GetEnumerator()) {
    $item = $entry.Value
    $reason = [string]($item.reason)
    if ([string]::IsNullOrWhiteSpace($reason)) { $reason = [string]($item.evidence_path) }
    $safeReason = $reason -replace "\|", "\|"
    $lines += "| $($entry.Key) | $([string]($item.status)) | $([string]($item.source_profile)) | $([string]($item.fresh)) | $safeReason |"
  }
  if (@($payload.issues).Count -gt 0) {
    $lines += ""
    $lines += "## Issues"
    foreach ($issue in $payload.issues) {
      $lines += "- $issue"
    }
  }
  $md = $lines -join [Environment]::NewLine
  Set-Content -Path $mdLatestPath -Value $md -Encoding UTF8
  Set-Content -Path $mdSnapshotPath -Value $md -Encoding UTF8

  return [ordered]@{
    json_latest = $jsonLatestPath
    json_snapshot = $jsonSnapshotPath
    md_latest = $mdLatestPath
    md_snapshot = $mdSnapshotPath
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
$sidecarRegressionQualityScript = Join-Path $PSScriptRoot "run_sidecar_regression_quality.ps1"
$sidecarPythonRustConsistencyScript = Join-Path $PSScriptRoot "run_sidecar_python_rust_consistency.ps1"
$regressionBaselineScript = Join-Path $PSScriptRoot "check_regression_baseline.ps1"
$rustTransformBenchGateSelfTestScript = Join-Path $PSScriptRoot "test_rust_transform_bench_gate.ps1"
$asyncBenchTrendScript = Join-Path $PSScriptRoot "check_async_bench_trend.ps1"
$rustTransformBenchGateScript = Join-Path $PSScriptRoot "check_rust_transform_bench_gate.ps1"
$rustNewOpsBenchGateScript = Join-Path $PSScriptRoot "check_rust_new_ops_bench_gate.ps1"
$openApiSdkSyncScript = Join-Path $PSScriptRoot "check_openapi_sdk_sync.ps1"
$frontendConvergenceScript = Join-Path $PSScriptRoot "check_frontend_convergence.ps1"
$workflowContractSyncScript = Join-Path $PSScriptRoot "check_workflow_contract_sync.ps1"
$governanceControlPlaneBoundaryScript = Join-Path $PSScriptRoot "check_governance_control_plane_boundary.ps1"
$governanceStoreSchemaVersionScript = Join-Path $PSScriptRoot "check_governance_store_schema_versions.ps1"
$localWorkflowStoreSchemaVersionScript = Join-Path $PSScriptRoot "check_local_workflow_store_schema_versions.ps1"
$templatePackContractSyncScript = Join-Path $PSScriptRoot "check_template_pack_contract_sync.ps1"
$localTemplateStorageContractSyncScript = Join-Path $PSScriptRoot "check_local_template_storage_contract_sync.ps1"
$offlineTemplateCatalogSyncScript = Join-Path $PSScriptRoot "check_offline_template_catalog_sync.ps1"
$nodeConfigSchemaCoverageScript = Join-Path $PSScriptRoot "check_node_config_schema_coverage.ps1"
$localNodeCatalogPolicyScript = Join-Path $PSScriptRoot "check_local_node_catalog_policy.ps1"
$operatorCatalogSyncScript = Join-Path $PSScriptRoot "check_operator_catalog_sync.ps1"
$cleaningRustV2RolloutScript = Join-Path $PSScriptRoot "check_cleaning_rust_v2_rollout.ps1"
$secretScanScript = Join-Path $PSScriptRoot "secret_scan.ps1"
$contractRustApiScript = Join-Path $PSScriptRoot "contract_test_rust_api.ps1"
$chaosTaskStoreScript = Join-Path $PSScriptRoot "chaos_task_store.ps1"
$sqlConnectivityScript = Join-Path $PSScriptRoot "check_sql_connectivity.ps1"
$nativeWinuiSmokeScript = Join-Path $PSScriptRoot "check_native_winui_smoke.ps1"
$cleanupScript = Join-Path $PSScriptRoot "clean_workspace_artifacts.ps1"
$restartServicesScript = Join-Path $PSScriptRoot "restart_services.ps1"
$accelServiceState = $null
$frontendVerificationDir = Join-Path $root "ops\logs\frontend_verification"
$frontendPrimaryEvidenceLatestPath = Join-Path $frontendVerificationDir "frontend_primary_verification_latest.json"
$frontendCompatibilityEvidenceLatestPath = Join-Path $frontendVerificationDir "frontend_compatibility_verification_latest.json"
$architectureScorecardDir = Join-Path $root "ops\logs\architecture"
$architectureScorecardLatestJsonPath = Join-Path $architectureScorecardDir "architecture_scorecard_latest.json"
$architectureScorecardLatestMdPath = Join-Path $architectureScorecardDir "architecture_scorecard_latest.md"
$architectureScorecardReleaseReadyLatestJsonPath = Join-Path $architectureScorecardDir "architecture_scorecard_release_ready_latest.json"
$architectureScorecardReleaseReadyLatestMdPath = Join-Path $architectureScorecardDir "architecture_scorecard_release_ready_latest.md"
$frontendVerificationStamp = Get-Date -Format "yyyyMMdd-HHmmss"
$normalizedCiProfile = if ([string]::IsNullOrWhiteSpace($CiProfile)) { "default" } else { $CiProfile.Trim().ToLowerInvariant() }
$workflowContractSyncCheckState = New-FrontendCheckState $(if ($SkipWorkflowContractSync) { "skipped" } else { "pending" }) $(if ($SkipWorkflowContractSync) { "skipped by SkipWorkflowContractSync" } else { "awaiting workflow contract sync gate" })
$governanceControlPlaneBoundaryCheckState = New-FrontendCheckState $(if ($SkipGovernanceControlPlaneBoundary) { "skipped" } else { "pending" }) $(if ($SkipGovernanceControlPlaneBoundary) { "skipped by SkipGovernanceControlPlaneBoundary" } else { "awaiting governance control plane boundary gate" })
$governanceStoreSchemaVersionCheckState = New-FrontendCheckState $(if ($SkipGovernanceStoreSchemaVersions) { "skipped" } else { "pending" }) $(if ($SkipGovernanceStoreSchemaVersions) { "skipped by SkipGovernanceStoreSchemaVersions" } else { "awaiting governance store schema version gate" })
$localWorkflowStoreSchemaVersionCheckState = New-FrontendCheckState $(if ($SkipLocalWorkflowStoreSchemaVersions) { "skipped" } else { "pending" }) $(if ($SkipLocalWorkflowStoreSchemaVersions) { "skipped by SkipLocalWorkflowStoreSchemaVersions" } else { "awaiting local workflow store schema version gate" })
$templatePackContractSyncCheckState = New-FrontendCheckState $(if ($SkipTemplatePackContractSync) { "skipped" } else { "pending" }) $(if ($SkipTemplatePackContractSync) { "skipped by SkipTemplatePackContractSync" } else { "awaiting template pack contract sync gate" })
$localTemplateStorageContractSyncCheckState = New-FrontendCheckState $(if ($SkipLocalTemplateStorageContractSync) { "skipped" } else { "pending" }) $(if ($SkipLocalTemplateStorageContractSync) { "skipped by SkipLocalTemplateStorageContractSync" } else { "awaiting local template storage contract sync gate" })
$offlineTemplateCatalogSyncCheckState = New-FrontendCheckState $(if ($SkipOfflineTemplateCatalogSync) { "skipped" } else { "pending" }) $(if ($SkipOfflineTemplateCatalogSync) { "skipped by SkipOfflineTemplateCatalogSync" } else { "awaiting offline template catalog sync gate" })
$nodeConfigSchemaCoverageCheckState = New-FrontendCheckState $(if ($SkipNodeConfigSchemaCoverage) { "skipped" } else { "pending" }) $(if ($SkipNodeConfigSchemaCoverage) { "skipped by SkipNodeConfigSchemaCoverage" } else { "awaiting node config schema coverage gate" })
$localNodeCatalogPolicyCheckState = New-FrontendCheckState $(if ($SkipLocalNodeCatalogPolicy) { "skipped" } else { "pending" }) $(if ($SkipLocalNodeCatalogPolicy) { "skipped by SkipLocalNodeCatalogPolicy" } else { "awaiting local node catalog policy gate" })
$operatorCatalogSyncCheckState = New-FrontendCheckState $(if ($SkipOperatorCatalogSync) { "skipped" } else { "pending" }) $(if ($SkipOperatorCatalogSync) { "skipped by SkipOperatorCatalogSync" } else { "awaiting operator catalog sync gate" })
$cleaningRustV2RolloutCheckState = New-FrontendCheckState $(if ($SkipCleaningRustV2RolloutGate) { "skipped" } else { "pending" }) $(if ($SkipCleaningRustV2RolloutGate) { "skipped by SkipCleaningRustV2RolloutGate" } else { "awaiting cleaning rust v2 rollout gate" })
$frontendConvergenceCheckState = New-FrontendCheckState $(if ($SkipFrontendConvergence) { "skipped" } else { "pending" }) $(if ($SkipFrontendConvergence) { "skipped by SkipFrontendConvergence" } else { "awaiting frontend convergence gate" })
$frontendPrimarySmokeCheckState = New-FrontendCheckState $(if ($SkipNativeWinuiSmoke) { "skipped" } else { "pending" }) $(if ($SkipNativeWinuiSmoke) { "skipped by SkipNativeWinuiSmoke" } else { "awaiting native winui smoke" })
$frontendCompatibilityPackagedCheckState = New-FrontendCheckState $(if ($SkipDesktopPackageTests) { "skipped" } else { "pending" }) $(if ($SkipDesktopPackageTests) { "moved out of current ci profile or skipped explicitly" } else { "awaiting Electron compatibility packaged startup check" })
$frontendCompatibilityLitePackagedCheckState = New-FrontendCheckState $(if ($SkipDesktopPackageTests) { "skipped" } else { "pending" }) $(if ($SkipDesktopPackageTests) { "moved out of current ci profile or skipped explicitly" } else { "awaiting Electron compatibility lite packaged startup check" })
$frontendPrimaryEvidencePaths = $null
$frontendCompatibilityEvidencePaths = $null
$architectureScorecardPaths = $null
$architectureReleaseReadyScorecardPaths = $null

function Publish-FrontendVerificationEvidence() {
  $primaryPayload = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    profile = $normalizedCiProfile
    source_script = "ops/scripts/ci_check.ps1"
    frontend_role = "primary"
    frontend = "winui"
    overall_status = Resolve-FrontendEvidenceOverall @($frontendConvergenceCheckState, $frontendPrimarySmokeCheckState)
    checks = [ordered]@{
      frontend_convergence = $frontendConvergenceCheckState
      native_winui_smoke = $frontendPrimarySmokeCheckState
    }
  }
  $compatibilityPayload = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    profile = $normalizedCiProfile
    source_script = "ops/scripts/ci_check.ps1"
    frontend_role = "compatibility"
    frontend = "electron"
    overall_status = Resolve-FrontendEvidenceOverall @($frontendConvergenceCheckState, $frontendCompatibilityPackagedCheckState, $frontendCompatibilityLitePackagedCheckState)
    checks = [ordered]@{
      frontend_convergence = $frontendConvergenceCheckState
      electron_packaged_startup = $frontendCompatibilityPackagedCheckState
      electron_lite_packaged_startup = $frontendCompatibilityLitePackagedCheckState
    }
  }
  $script:frontendPrimaryEvidencePaths = Write-FrontendEvidenceSnapshot -Name "frontend_primary_verification" -Payload $primaryPayload -Directory $frontendVerificationDir -Stamp $frontendVerificationStamp
  $script:frontendCompatibilityEvidencePaths = Write-FrontendEvidenceSnapshot -Name "frontend_compatibility_verification" -Payload $compatibilityPayload -Directory $frontendVerificationDir -Stamp $frontendVerificationStamp
  Publish-ArchitectureScorecard
}

function Publish-ArchitectureScorecard() {
  $primarySummary = [ordered]@{
    status = Resolve-FrontendEvidenceOverall @($frontendConvergenceCheckState, $frontendPrimarySmokeCheckState)
    reason = "primary frontend verification summary"
    evidence_path = $frontendPrimaryEvidenceLatestPath
    generated_at = (Get-Date).ToString("s")
    profile = $normalizedCiProfile
    checks = [ordered]@{
      frontend_convergence = $frontendConvergenceCheckState
      native_winui_smoke = $frontendPrimarySmokeCheckState
    }
  }
  $compatibilitySummary = [ordered]@{
    status = Resolve-FrontendEvidenceOverall @($frontendConvergenceCheckState, $frontendCompatibilityPackagedCheckState, $frontendCompatibilityLitePackagedCheckState)
    reason = "compatibility frontend verification summary"
    evidence_path = $frontendCompatibilityEvidenceLatestPath
    generated_at = (Get-Date).ToString("s")
    profile = $normalizedCiProfile
    checks = [ordered]@{
      frontend_convergence = $frontendConvergenceCheckState
      electron_packaged_startup = $frontendCompatibilityPackagedCheckState
      electron_lite_packaged_startup = $frontendCompatibilityLitePackagedCheckState
    }
  }
  $payload = [ordered]@{
    generated_at = (Get-Date).ToString("s")
    profile = $normalizedCiProfile
    source_script = "ops/scripts/ci_check.ps1"
    overall_status = Resolve-ArchitectureScorecardOverall @(
      $workflowContractSyncCheckState,
      $governanceControlPlaneBoundaryCheckState,
      $governanceStoreSchemaVersionCheckState,
      $localWorkflowStoreSchemaVersionCheckState,
      $templatePackContractSyncCheckState,
      $localTemplateStorageContractSyncCheckState,
      $offlineTemplateCatalogSyncCheckState,
      $nodeConfigSchemaCoverageCheckState,
      $localNodeCatalogPolicyCheckState,
      $operatorCatalogSyncCheckState,
      $cleaningRustV2RolloutCheckState,
      $frontendConvergenceCheckState,
      [ordered]@{ status = $primarySummary.status },
      [ordered]@{ status = $compatibilitySummary.status }
    )
    boundaries = [ordered]@{
      workflow_contract_sync = $workflowContractSyncCheckState
      governance_control_plane_boundary = $governanceControlPlaneBoundaryCheckState
      governance_store_schema_versions = $governanceStoreSchemaVersionCheckState
      local_workflow_store_schema_versions = $localWorkflowStoreSchemaVersionCheckState
      template_pack_contract_sync = $templatePackContractSyncCheckState
      local_template_storage_contract_sync = $localTemplateStorageContractSyncCheckState
      offline_template_catalog_sync = $offlineTemplateCatalogSyncCheckState
      node_config_schema_coverage = $nodeConfigSchemaCoverageCheckState
      local_node_catalog_policy = $localNodeCatalogPolicyCheckState
      operator_catalog_sync = $operatorCatalogSyncCheckState
      cleaning_rust_v2_rollout = $cleaningRustV2RolloutCheckState
      frontend_convergence = $frontendConvergenceCheckState
    }
    frontend = [ordered]@{
      primary = $primarySummary
      compatibility = $compatibilitySummary
    }
  }
  $script:architectureScorecardPaths = Write-ArchitectureScorecardSnapshot -Payload $payload -Directory $architectureScorecardDir -Stamp $frontendVerificationStamp -ProfileLabel $normalizedCiProfile
  $script:architectureReleaseReadyScorecardPaths = Write-ArchitectureReleaseReadyScorecard -Directory $architectureScorecardDir -Stamp $frontendVerificationStamp
}

Publish-FrontendVerificationEvidence

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

if (-not $SkipFrontendConvergence) {
  if (-not (Test-Path $frontendConvergenceScript)) {
    Set-FrontendCheckState $frontendConvergenceCheckState "failed" "frontend convergence script missing"
    Publish-FrontendVerificationEvidence
    throw "frontend convergence script not found: $frontendConvergenceScript"
  }
  Info "running frontend convergence checks"
  powershell -ExecutionPolicy Bypass -File $frontendConvergenceScript
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $frontendConvergenceCheckState "failed" "frontend convergence checks failed"
    Publish-FrontendVerificationEvidence
    throw "frontend convergence checks failed"
  }
  Set-FrontendCheckState $frontendConvergenceCheckState "passed" "frontend convergence checks passed"
  Publish-FrontendVerificationEvidence
  Ok "frontend convergence checks passed"
} else {
  Set-FrontendCheckState $frontendConvergenceCheckState "skipped" "skipped by SkipFrontendConvergence"
  Publish-FrontendVerificationEvidence
  Warn "skip frontend convergence checks"
}

if (-not $SkipWorkflowContractSync) {
  if (-not (Test-Path $workflowContractSyncScript)) {
    Set-FrontendCheckState $workflowContractSyncCheckState "failed" "workflow contract sync script missing"
    Publish-ArchitectureScorecard
    throw "workflow contract sync script not found: $workflowContractSyncScript"
  }
  Info "running workflow contract sync checks"
  $workflowContractSyncOutput = powershell -ExecutionPolicy Bypass -File $workflowContractSyncScript 2>&1
  $workflowContractSummary = Parse-JsonLineFromOutput $workflowContractSyncOutput
  $workflowContractDetails = New-WorkflowContractSyncDetails $workflowContractSummary
  if ($workflowContractDetails) {
    $workflowContractSyncCheckState.details = $workflowContractDetails
  }
  if ($null -ne $workflowContractSyncOutput) {
    $workflowContractSyncOutput | ForEach-Object { $_ }
  }
  $workflowContractSyncExitCode = $LASTEXITCODE
  if ($workflowContractSyncExitCode -ne 0) {
    Set-FrontendCheckState $workflowContractSyncCheckState "failed" (Get-WorkflowContractSyncReason -Summary $workflowContractSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "workflow contract sync checks failed"
  }
  if ($workflowContractSummary) {
    Set-FrontendCheckState $workflowContractSyncCheckState "passed" (Get-WorkflowContractSyncReason -Summary $workflowContractSummary -Status "passed")
  } else {
    Set-FrontendCheckState $workflowContractSyncCheckState "passed" "workflow contract sync checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "workflow contract sync checks passed"
} else {
  Set-FrontendCheckState $workflowContractSyncCheckState "skipped" "skipped by SkipWorkflowContractSync"
  Publish-ArchitectureScorecard
  Warn "skip workflow contract sync checks"
}

if (-not $SkipGovernanceControlPlaneBoundary) {
  if (-not (Test-Path $governanceControlPlaneBoundaryScript)) {
    Set-FrontendCheckState $governanceControlPlaneBoundaryCheckState "failed" "governance control plane boundary script missing"
    Publish-ArchitectureScorecard
    throw "governance control plane boundary script not found: $governanceControlPlaneBoundaryScript"
  }
  Info "running governance control plane boundary checks"
  $governanceControlPlaneBoundaryOutput = powershell -ExecutionPolicy Bypass -File $governanceControlPlaneBoundaryScript 2>&1
  $governanceControlPlaneBoundarySummary = Parse-JsonLineFromOutput $governanceControlPlaneBoundaryOutput
  $governanceControlPlaneBoundaryDetails = New-GovernanceControlPlaneBoundaryDetails $governanceControlPlaneBoundarySummary
  if ($governanceControlPlaneBoundaryDetails) {
    $governanceControlPlaneBoundaryCheckState.details = $governanceControlPlaneBoundaryDetails
  }
  if ($null -ne $governanceControlPlaneBoundaryOutput) {
    $governanceControlPlaneBoundaryOutput | ForEach-Object { $_ }
  }
  $governanceControlPlaneBoundaryExitCode = $LASTEXITCODE
  if ($governanceControlPlaneBoundaryExitCode -ne 0) {
    Set-FrontendCheckState $governanceControlPlaneBoundaryCheckState "failed" (Get-GovernanceControlPlaneBoundaryReason -Summary $governanceControlPlaneBoundarySummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "governance control plane boundary checks failed"
  }
  if ($governanceControlPlaneBoundarySummary) {
    Set-FrontendCheckState $governanceControlPlaneBoundaryCheckState "passed" (Get-GovernanceControlPlaneBoundaryReason -Summary $governanceControlPlaneBoundarySummary -Status "passed")
  } else {
    Set-FrontendCheckState $governanceControlPlaneBoundaryCheckState "passed" "governance control plane boundary checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "governance control plane boundary checks passed"
} else {
  Set-FrontendCheckState $governanceControlPlaneBoundaryCheckState "skipped" "skipped by SkipGovernanceControlPlaneBoundary"
  Publish-ArchitectureScorecard
  Warn "skip governance control plane boundary checks"
}

if (-not $SkipGovernanceStoreSchemaVersions) {
  if (-not (Test-Path $governanceStoreSchemaVersionScript)) {
    Set-FrontendCheckState $governanceStoreSchemaVersionCheckState "failed" "governance store schema version script missing"
    Publish-ArchitectureScorecard
    throw "governance store schema version script not found: $governanceStoreSchemaVersionScript"
  }
  Info "running governance store schema version checks"
  $governanceStoreSchemaVersionOutput = powershell -ExecutionPolicy Bypass -File $governanceStoreSchemaVersionScript 2>&1
  $governanceStoreSchemaVersionSummary = Parse-JsonLineFromOutput $governanceStoreSchemaVersionOutput
  $governanceStoreSchemaVersionDetails = New-GovernanceStoreSchemaVersionDetails $governanceStoreSchemaVersionSummary
  if ($governanceStoreSchemaVersionDetails) {
    $governanceStoreSchemaVersionCheckState.details = $governanceStoreSchemaVersionDetails
  }
  if ($null -ne $governanceStoreSchemaVersionOutput) {
    $governanceStoreSchemaVersionOutput | ForEach-Object { $_ }
  }
  $governanceStoreSchemaVersionExitCode = $LASTEXITCODE
  if ($governanceStoreSchemaVersionExitCode -ne 0) {
    Set-FrontendCheckState $governanceStoreSchemaVersionCheckState "failed" (Get-GovernanceStoreSchemaVersionReason -Summary $governanceStoreSchemaVersionSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "governance store schema version checks failed"
  }
  if ($governanceStoreSchemaVersionSummary) {
    Set-FrontendCheckState $governanceStoreSchemaVersionCheckState "passed" (Get-GovernanceStoreSchemaVersionReason -Summary $governanceStoreSchemaVersionSummary -Status "passed")
  } else {
    Set-FrontendCheckState $governanceStoreSchemaVersionCheckState "passed" "governance store schema version checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "governance store schema version checks passed"
} else {
  Set-FrontendCheckState $governanceStoreSchemaVersionCheckState "skipped" "skipped by SkipGovernanceStoreSchemaVersions"
  Publish-ArchitectureScorecard
  Warn "skip governance store schema version checks"
}

if (-not $SkipLocalWorkflowStoreSchemaVersions) {
  if (-not (Test-Path $localWorkflowStoreSchemaVersionScript)) {
    Set-FrontendCheckState $localWorkflowStoreSchemaVersionCheckState "failed" "local workflow store schema version script missing"
    Publish-ArchitectureScorecard
    throw "local workflow store schema version script not found: $localWorkflowStoreSchemaVersionScript"
  }
  Info "running local workflow store schema version checks"
  $localWorkflowStoreSchemaVersionOutput = powershell -ExecutionPolicy Bypass -File $localWorkflowStoreSchemaVersionScript 2>&1
  $localWorkflowStoreSchemaVersionSummary = Parse-JsonLineFromOutput $localWorkflowStoreSchemaVersionOutput
  $localWorkflowStoreSchemaVersionDetails = New-LocalWorkflowStoreSchemaVersionDetails $localWorkflowStoreSchemaVersionSummary
  if ($localWorkflowStoreSchemaVersionDetails) {
    $localWorkflowStoreSchemaVersionCheckState.details = $localWorkflowStoreSchemaVersionDetails
  }
  if ($null -ne $localWorkflowStoreSchemaVersionOutput) {
    $localWorkflowStoreSchemaVersionOutput | ForEach-Object { $_ }
  }
  $localWorkflowStoreSchemaVersionExitCode = $LASTEXITCODE
  if ($localWorkflowStoreSchemaVersionExitCode -ne 0) {
    Set-FrontendCheckState $localWorkflowStoreSchemaVersionCheckState "failed" (Get-LocalWorkflowStoreSchemaVersionReason -Summary $localWorkflowStoreSchemaVersionSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "local workflow store schema version checks failed"
  }
  if ($localWorkflowStoreSchemaVersionSummary) {
    Set-FrontendCheckState $localWorkflowStoreSchemaVersionCheckState "passed" (Get-LocalWorkflowStoreSchemaVersionReason -Summary $localWorkflowStoreSchemaVersionSummary -Status "passed")
  } else {
    Set-FrontendCheckState $localWorkflowStoreSchemaVersionCheckState "passed" "local workflow store schema version checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "local workflow store schema version checks passed"
} else {
  Set-FrontendCheckState $localWorkflowStoreSchemaVersionCheckState "skipped" "skipped by SkipLocalWorkflowStoreSchemaVersions"
  Publish-ArchitectureScorecard
  Warn "skip local workflow store schema version checks"
}

if (-not $SkipTemplatePackContractSync) {
  if (-not (Test-Path $templatePackContractSyncScript)) {
    Set-FrontendCheckState $templatePackContractSyncCheckState "failed" "template pack contract sync script missing"
    Publish-ArchitectureScorecard
    throw "template pack contract sync script not found: $templatePackContractSyncScript"
  }
  Info "running template pack contract sync checks"
  $templatePackContractSyncOutput = powershell -ExecutionPolicy Bypass -File $templatePackContractSyncScript 2>&1
  $templatePackContractSummary = Parse-JsonLineFromOutput $templatePackContractSyncOutput
  $templatePackContractDetails = New-TemplatePackContractDetails $templatePackContractSummary
  if ($templatePackContractDetails) {
    $templatePackContractSyncCheckState.details = $templatePackContractDetails
  }
  if ($null -ne $templatePackContractSyncOutput) {
    $templatePackContractSyncOutput | ForEach-Object { $_ }
  }
  $templatePackContractSyncExitCode = $LASTEXITCODE
  if ($templatePackContractSyncExitCode -ne 0) {
    Set-FrontendCheckState $templatePackContractSyncCheckState "failed" (Get-TemplatePackContractReason -Summary $templatePackContractSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "template pack contract sync checks failed"
  }
  if ($templatePackContractSummary) {
    Set-FrontendCheckState $templatePackContractSyncCheckState "passed" (Get-TemplatePackContractReason -Summary $templatePackContractSummary -Status "passed")
  } else {
    Set-FrontendCheckState $templatePackContractSyncCheckState "passed" "template pack contract sync checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "template pack contract sync checks passed"
} else {
  Set-FrontendCheckState $templatePackContractSyncCheckState "skipped" "skipped by SkipTemplatePackContractSync"
  Publish-ArchitectureScorecard
  Warn "skip template pack contract sync checks"
}

if (-not $SkipLocalTemplateStorageContractSync) {
  if (-not (Test-Path $localTemplateStorageContractSyncScript)) {
    Set-FrontendCheckState $localTemplateStorageContractSyncCheckState "failed" "local template storage contract sync script missing"
    Publish-ArchitectureScorecard
    throw "local template storage contract sync script not found: $localTemplateStorageContractSyncScript"
  }
  Info "running local template storage contract sync checks"
  $localTemplateStorageContractSyncOutput = powershell -ExecutionPolicy Bypass -File $localTemplateStorageContractSyncScript 2>&1
  $localTemplateStorageContractSummary = Parse-JsonLineFromOutput $localTemplateStorageContractSyncOutput
  $localTemplateStorageContractDetails = New-LocalTemplateStorageContractDetails $localTemplateStorageContractSummary
  if ($localTemplateStorageContractDetails) {
    $localTemplateStorageContractSyncCheckState.details = $localTemplateStorageContractDetails
  }
  if ($null -ne $localTemplateStorageContractSyncOutput) {
    $localTemplateStorageContractSyncOutput | ForEach-Object { $_ }
  }
  $localTemplateStorageContractSyncExitCode = $LASTEXITCODE
  if ($localTemplateStorageContractSyncExitCode -ne 0) {
    Set-FrontendCheckState $localTemplateStorageContractSyncCheckState "failed" (Get-LocalTemplateStorageContractReason -Summary $localTemplateStorageContractSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "local template storage contract sync checks failed"
  }
  if ($localTemplateStorageContractSummary) {
    Set-FrontendCheckState $localTemplateStorageContractSyncCheckState "passed" (Get-LocalTemplateStorageContractReason -Summary $localTemplateStorageContractSummary -Status "passed")
  } else {
    Set-FrontendCheckState $localTemplateStorageContractSyncCheckState "passed" "local template storage contract sync checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "local template storage contract sync checks passed"
} else {
  Set-FrontendCheckState $localTemplateStorageContractSyncCheckState "skipped" "skipped by SkipLocalTemplateStorageContractSync"
  Publish-ArchitectureScorecard
  Warn "skip local template storage contract sync checks"
}

if (-not $SkipOfflineTemplateCatalogSync) {
  if (-not (Test-Path $offlineTemplateCatalogSyncScript)) {
    Set-FrontendCheckState $offlineTemplateCatalogSyncCheckState "failed" "offline template catalog sync script missing"
    Publish-ArchitectureScorecard
    throw "offline template catalog sync script not found: $offlineTemplateCatalogSyncScript"
  }
  Info "running offline template catalog sync checks"
  $offlineTemplateCatalogSyncOutput = powershell -ExecutionPolicy Bypass -File $offlineTemplateCatalogSyncScript 2>&1
  $offlineTemplateCatalogSummary = Parse-JsonLineFromOutput $offlineTemplateCatalogSyncOutput
  $offlineTemplateCatalogDetails = New-OfflineTemplateCatalogDetails $offlineTemplateCatalogSummary
  if ($offlineTemplateCatalogDetails) {
    $offlineTemplateCatalogSyncCheckState.details = $offlineTemplateCatalogDetails
  }
  if ($null -ne $offlineTemplateCatalogSyncOutput) {
    $offlineTemplateCatalogSyncOutput | ForEach-Object { $_ }
  }
  $offlineTemplateCatalogSyncExitCode = $LASTEXITCODE
  if ($offlineTemplateCatalogSyncExitCode -ne 0) {
    Set-FrontendCheckState $offlineTemplateCatalogSyncCheckState "failed" (Get-OfflineTemplateCatalogReason -Summary $offlineTemplateCatalogSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "offline template catalog sync checks failed"
  }
  if ($offlineTemplateCatalogSummary) {
    Set-FrontendCheckState $offlineTemplateCatalogSyncCheckState "passed" (Get-OfflineTemplateCatalogReason -Summary $offlineTemplateCatalogSummary -Status "passed")
  } else {
    Set-FrontendCheckState $offlineTemplateCatalogSyncCheckState "passed" "offline template catalog sync checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "offline template catalog sync checks passed"
} else {
  Set-FrontendCheckState $offlineTemplateCatalogSyncCheckState "skipped" "skipped by SkipOfflineTemplateCatalogSync"
  Publish-ArchitectureScorecard
  Warn "skip offline template catalog sync checks"
}

if (-not $SkipNodeConfigSchemaCoverage) {
  if (-not (Test-Path $nodeConfigSchemaCoverageScript)) {
    Set-FrontendCheckState $nodeConfigSchemaCoverageCheckState "failed" "node config schema coverage script missing"
    Publish-ArchitectureScorecard
    throw "node config schema coverage script not found: $nodeConfigSchemaCoverageScript"
  }
  Info "running node config schema coverage checks"
  $nodeConfigSchemaCoverageOutput = powershell -ExecutionPolicy Bypass -File $nodeConfigSchemaCoverageScript 2>&1
  $nodeConfigSummary = Parse-JsonLineFromOutput $nodeConfigSchemaCoverageOutput
  $nodeConfigDetails = New-NodeConfigSchemaCoverageDetails $nodeConfigSummary
  if ($nodeConfigDetails) {
    $nodeConfigSchemaCoverageCheckState.details = $nodeConfigDetails
  }
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $nodeConfigSchemaCoverageCheckState "failed" (Get-NodeConfigSchemaCoverageReason -Summary $nodeConfigSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "node config schema coverage checks failed"
  }
  if ($nodeConfigSummary) {
    Set-FrontendCheckState $nodeConfigSchemaCoverageCheckState "passed" (Get-NodeConfigSchemaCoverageReason -Summary $nodeConfigSummary -Status "passed")
  } else {
    Set-FrontendCheckState $nodeConfigSchemaCoverageCheckState "passed" "node config schema coverage checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "node config schema coverage checks passed"
} else {
  Set-FrontendCheckState $nodeConfigSchemaCoverageCheckState "skipped" "skipped by SkipNodeConfigSchemaCoverage"
  Publish-ArchitectureScorecard
  Warn "skip node config schema coverage checks"
}

if (-not $SkipLocalNodeCatalogPolicy) {
  if (-not (Test-Path $localNodeCatalogPolicyScript)) {
    Set-FrontendCheckState $localNodeCatalogPolicyCheckState "failed" "local node catalog policy script missing"
    Publish-ArchitectureScorecard
    throw "local node catalog policy script not found: $localNodeCatalogPolicyScript"
  }
  Info "running local node catalog policy checks"
  $localNodeCatalogPolicyOutput = powershell -ExecutionPolicy Bypass -File $localNodeCatalogPolicyScript 2>&1
  $localNodeCatalogPolicySummary = Parse-JsonLineFromOutput $localNodeCatalogPolicyOutput
  if ($localNodeCatalogPolicySummary) {
    $localNodeCatalogPolicyCheckState.details = New-LocalNodeCatalogPolicyDetails $localNodeCatalogPolicySummary
  }
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $localNodeCatalogPolicyCheckState "failed" (Get-LocalNodeCatalogPolicyReason -Summary $localNodeCatalogPolicySummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "local node catalog policy checks failed"
  }
  if ($localNodeCatalogPolicySummary) {
    Set-FrontendCheckState $localNodeCatalogPolicyCheckState "passed" (Get-LocalNodeCatalogPolicyReason -Summary $localNodeCatalogPolicySummary -Status "passed")
  } else {
    Set-FrontendCheckState $localNodeCatalogPolicyCheckState "passed" "local node catalog policy checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "local node catalog policy checks passed"
} else {
  Set-FrontendCheckState $localNodeCatalogPolicyCheckState "skipped" "skipped by SkipLocalNodeCatalogPolicy"
  Publish-ArchitectureScorecard
  Warn "skip local node catalog policy checks"
}

if (-not $SkipOperatorCatalogSync) {
  if (-not (Test-Path $operatorCatalogSyncScript)) {
    Set-FrontendCheckState $operatorCatalogSyncCheckState "failed" "operator catalog sync script missing"
    Publish-ArchitectureScorecard
    throw "operator catalog sync script not found: $operatorCatalogSyncScript"
  }
  Info "running operator catalog sync checks"
  $operatorCatalogSyncOutput = powershell -ExecutionPolicy Bypass -File $operatorCatalogSyncScript 2>&1
  $operatorCatalogSummary = Parse-JsonLineFromOutput $operatorCatalogSyncOutput
  $operatorCatalogDetails = New-OperatorCatalogSyncDetails $operatorCatalogSummary
  if ($operatorCatalogDetails) {
    $operatorCatalogSyncCheckState.details = $operatorCatalogDetails
  }
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $operatorCatalogSyncCheckState "failed" (Get-OperatorCatalogSyncReason -Summary $operatorCatalogSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "operator catalog sync checks failed"
  }
  if ($operatorCatalogSummary) {
    Set-FrontendCheckState $operatorCatalogSyncCheckState "passed" (Get-OperatorCatalogSyncReason -Summary $operatorCatalogSummary -Status "passed")
  } else {
    Set-FrontendCheckState $operatorCatalogSyncCheckState "passed" "operator catalog sync checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "operator catalog sync checks passed"
} else {
  Set-FrontendCheckState $operatorCatalogSyncCheckState "skipped" "skipped by SkipOperatorCatalogSync"
  Publish-ArchitectureScorecard
  Warn "skip operator catalog sync checks"
}

if (-not $SkipCleaningRustV2RolloutGate) {
  if (-not (Test-Path $cleaningRustV2RolloutScript)) {
    Set-FrontendCheckState $cleaningRustV2RolloutCheckState "failed" "cleaning rust v2 rollout script missing"
    Publish-ArchitectureScorecard
    throw "cleaning rust v2 rollout script not found: $cleaningRustV2RolloutScript"
  }
  Info "running cleaning rust v2 rollout checks"
  $cleaningRustV2RolloutOutput = powershell -ExecutionPolicy Bypass -File $cleaningRustV2RolloutScript 2>&1
  $cleaningRustV2RolloutSummary = Parse-JsonLineFromOutput $cleaningRustV2RolloutOutput
  $cleaningRustV2RolloutDetails = New-CleaningRustV2RolloutDetails $cleaningRustV2RolloutSummary
  if ($cleaningRustV2RolloutDetails) {
    $cleaningRustV2RolloutCheckState.details = $cleaningRustV2RolloutDetails
  }
  if ($null -ne $cleaningRustV2RolloutOutput) {
    $cleaningRustV2RolloutOutput | ForEach-Object { $_ }
  }
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $cleaningRustV2RolloutCheckState "failed" (Get-CleaningRustV2RolloutReason -Summary $cleaningRustV2RolloutSummary -Status "failed")
    Publish-ArchitectureScorecard
    throw "cleaning rust v2 rollout checks failed"
  }
  if ($cleaningRustV2RolloutSummary) {
    Set-FrontendCheckState $cleaningRustV2RolloutCheckState "passed" (Get-CleaningRustV2RolloutReason -Summary $cleaningRustV2RolloutSummary -Status "passed")
  } else {
    Set-FrontendCheckState $cleaningRustV2RolloutCheckState "passed" "cleaning rust v2 rollout checks passed"
  }
  Publish-ArchitectureScorecard
  Ok "cleaning rust v2 rollout checks passed"
} else {
  Set-FrontendCheckState $cleaningRustV2RolloutCheckState "skipped" "skipped by SkipCleaningRustV2RolloutGate"
  Publish-ArchitectureScorecard
  Warn "skip cleaning rust v2 rollout checks"
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
  if ($LASTEXITCODE -ne 0) {
    throw "accel-rust tests failed"
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
  if ($LASTEXITCODE -ne 0) {
    throw "base-java tests failed"
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
  if ($LASTEXITCODE -ne 0) {
    throw "glue-python tests failed"
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

if (-not $SkipSidecarRegressionQuality) {
  if (-not (Test-Path $sidecarRegressionQualityScript)) {
    throw "sidecar regression quality script not found: $sidecarRegressionQualityScript"
  }
  Info "running sidecar regression quality checks"
  powershell -ExecutionPolicy Bypass -File $sidecarRegressionQualityScript
  if ($LASTEXITCODE -ne 0) {
    throw "sidecar regression quality checks failed"
  }
  Ok "sidecar regression quality checks passed"
} else {
  Warn "skip sidecar regression quality checks"
}

if (-not $SkipSidecarPythonRustConsistency) {
  if (-not (Test-Path $sidecarPythonRustConsistencyScript)) {
    throw "sidecar python/rust consistency script not found: $sidecarPythonRustConsistencyScript"
  }
  Info "running sidecar python/rust consistency checks"
  $sidecarConsistencyServiceState = $null
  try {
    $sidecarConsistencyServiceState = Ensure-AccelRustService -RustDir $rustDir -AccelUrl "http://127.0.0.1:18082"
    powershell -ExecutionPolicy Bypass -File $sidecarPythonRustConsistencyScript -RequireAccel -AccelUrl "http://127.0.0.1:18082"
    if ($LASTEXITCODE -ne 0) {
      throw "sidecar python/rust consistency checks failed"
    }
  }
  finally {
    Stop-AccelRustService $sidecarConsistencyServiceState
  }
  Ok "sidecar python/rust consistency checks passed"
} else {
  Warn "skip sidecar python/rust consistency checks"
}

if (-not (Test-Path $rustTransformBenchGateSelfTestScript)) {
  throw "rust transform benchmark gate self-test script not found: $rustTransformBenchGateSelfTestScript"
}
Info "running rust transform benchmark gate self-test"
powershell -ExecutionPolicy Bypass -File $rustTransformBenchGateSelfTestScript
if ($LASTEXITCODE -ne 0) {
  throw "rust transform benchmark gate self-test failed"
}
Ok "rust transform benchmark gate self-test passed"

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

  $isCi = Test-IsCiEnvironment
  $desktopAcceptanceBootstrapTimeoutSeconds = if ($isCi) { 600 } else { 90 }
  $runDesktopRealSampleAcceptance = -not $SkipDesktopRealSampleAcceptance
  $runDesktopFinanceTemplateAcceptance = -not $SkipDesktopFinanceTemplateAcceptance
  if ($runDesktopRealSampleAcceptance -or $runDesktopFinanceTemplateAcceptance) {
    if (Test-Path $restartServicesScript) {
      Info "ensuring base/glue/accel services are healthy before desktop acceptance"
      powershell -ExecutionPolicy Bypass -File $restartServicesScript -EnvFile $EnvFile -TimeoutSeconds $desktopAcceptanceBootstrapTimeoutSeconds
      if ($LASTEXITCODE -ne 0) {
        if ($isCi) {
          throw "service bootstrap before desktop acceptance failed"
        }
        Warn "service bootstrap before desktop acceptance failed in local mode; skip desktop acceptance checks"
        $runDesktopRealSampleAcceptance = $false
        $runDesktopFinanceTemplateAcceptance = $false
      } else {
        Ok "service bootstrap before desktop acceptance passed"
      }
    } else {
      Warn "restart services script not found, continue without desktop acceptance bootstrap: $restartServicesScript"
    }
  }

  if ($runDesktopRealSampleAcceptance) {
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

  if ($runDesktopFinanceTemplateAcceptance) {
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

if (-not $SkipNativeWinuiSmoke) {
  if (-not (Test-Path $nativeWinuiSmokeScript)) {
    Set-FrontendCheckState $frontendPrimarySmokeCheckState "failed" "native winui smoke script missing"
    Publish-FrontendVerificationEvidence
    throw "native winui smoke script not found: $nativeWinuiSmokeScript"
  }
  $isCi = Test-IsCiEnvironment
  if ($isCi) {
    Set-FrontendCheckState $frontendPrimarySmokeCheckState "skipped" "skipped in CI environment"
    Publish-FrontendVerificationEvidence
    Warn "skip native winui primary frontend smoke in CI environment"
  } else {
    Info "running native winui primary frontend smoke check"
    powershell -ExecutionPolicy Bypass -File $nativeWinuiSmokeScript -Root $root
    if ($LASTEXITCODE -ne 0) {
      Set-FrontendCheckState $frontendPrimarySmokeCheckState "failed" "native winui primary frontend smoke check failed"
      Publish-FrontendVerificationEvidence
      throw "native winui primary frontend smoke check failed"
    }
    Set-FrontendCheckState $frontendPrimarySmokeCheckState "passed" "native winui primary frontend smoke check passed"
    Publish-FrontendVerificationEvidence
    Ok "native winui primary frontend smoke check passed"
  }
} else {
  Set-FrontendCheckState $frontendPrimarySmokeCheckState "skipped" "skipped by SkipNativeWinuiSmoke"
  Publish-FrontendVerificationEvidence
  Warn "skip native winui primary frontend smoke check"
}

if (-not $SkipDesktopPackageTests) {
  if (-not (Test-Path $desktopPkgCheckScript)) {
    Set-FrontendCheckState $frontendCompatibilityPackagedCheckState "failed" "desktop packaged startup check script missing"
    Publish-FrontendVerificationEvidence
    throw "desktop packaged startup check script not found: $desktopPkgCheckScript"
  }
  if (-not (Test-Path $desktopLitePkgCheckScript)) {
    Set-FrontendCheckState $frontendCompatibilityLitePackagedCheckState "failed" "desktop lite packaged startup check script missing"
    Publish-FrontendVerificationEvidence
    throw "desktop lite packaged startup check script not found: $desktopLitePkgCheckScript"
  }
  Info "running Electron compatibility packaged startup check"
  powershell -ExecutionPolicy Bypass -File $desktopPkgCheckScript -DesktopDir $desktopDir
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $frontendCompatibilityPackagedCheckState "failed" "Electron compatibility packaged startup check failed"
    Publish-FrontendVerificationEvidence
    throw "Electron compatibility packaged startup check failed"
  }
  Set-FrontendCheckState $frontendCompatibilityPackagedCheckState "passed" "Electron compatibility packaged startup check passed"
  Publish-FrontendVerificationEvidence
  Ok "Electron compatibility packaged startup check passed"

  Info "running Electron compatibility lite packaged startup check"
  powershell -ExecutionPolicy Bypass -File $desktopLitePkgCheckScript -DesktopDir $desktopDir
  if ($LASTEXITCODE -ne 0) {
    Set-FrontendCheckState $frontendCompatibilityLitePackagedCheckState "failed" "Electron compatibility lite packaged startup check failed"
    Publish-FrontendVerificationEvidence
    throw "Electron compatibility lite packaged startup check failed"
  }
  Set-FrontendCheckState $frontendCompatibilityLitePackagedCheckState "passed" "Electron compatibility lite packaged startup check passed"
  Publish-FrontendVerificationEvidence
  Ok "Electron compatibility lite packaged startup check passed"
} else {
  Set-FrontendCheckState $frontendCompatibilityPackagedCheckState "skipped" "moved out of current ci profile or skipped explicitly"
  Set-FrontendCheckState $frontendCompatibilityLitePackagedCheckState "skipped" "moved out of current ci profile or skipped explicitly"
  Publish-FrontendVerificationEvidence
  Warn "skip Electron compatibility packaged startup checks"
}

if (-not $SkipSmoke) {
  if (-not (Test-Path $smokeScript)) {
    throw "smoke script not found: $smokeScript"
  }
  $isCi = Test-IsCiEnvironment
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
if ($frontendPrimaryEvidencePaths) {
  Info ("frontend primary verification evidence: " + $frontendPrimaryEvidenceLatestPath)
}
if ($frontendCompatibilityEvidencePaths) {
  Info ("frontend compatibility verification evidence: " + $frontendCompatibilityEvidenceLatestPath)
}
if ($architectureScorecardPaths) {
  Info ("architecture scorecard json: " + $architectureScorecardLatestJsonPath)
  Info ("architecture scorecard md: " + $architectureScorecardLatestMdPath)
}
if ($architectureReleaseReadyScorecardPaths) {
  Info ("architecture release-ready scorecard json: " + $architectureScorecardReleaseReadyLatestJsonPath)
  Info ("architecture release-ready scorecard md: " + $architectureScorecardReleaseReadyLatestMdPath)
}
