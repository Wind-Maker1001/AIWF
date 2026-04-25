param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$charterPath = Join-Path $RepoRoot "docs\architecture_authority_charter_20260425.md"
$docsReadmePath = Join-Path $RepoRoot "docs\README.md"
$repoReadmePath = Join-Path $RepoRoot "README.md"
$inventoryPath = Join-Path $RepoRoot "contracts\governance\fallback_inventory.v1.json"

$issues = @()

if (-not (Test-Path $charterPath)) {
  $issues += "architecture authority charter missing"
}
if (-not (Test-Path $inventoryPath)) {
  $issues += "fallback inventory manifest missing"
}

$requiredLinkTargets = @(
  @{ Path = $docsReadmePath; Pattern = "architecture_authority_charter_20260425.md" },
  @{ Path = $repoReadmePath; Pattern = "architecture_authority_charter_20260425.md" }
)

foreach ($item in $requiredLinkTargets) {
  if (-not (Test-Path $item.Path)) {
    $issues += "required readme missing: $($item.Path)"
    continue
  }
  $text = Get-Content -Raw -Encoding UTF8 $item.Path
  if ($text -notmatch [regex]::Escape($item.Pattern)) {
    $issues += "$($item.Path) missing charter link: $($item.Pattern)"
  }
}

$charterPatterns = @(
  '`apps/base-java` owns the lifecycle plane',
  '`apps/glue-python` owns the governance plane',
  '`apps/accel-rust` owns workflow / node-config / operator executable semantics',
  '`workflow_definition` is the canonical workflow payload field',
  'WorkflowExecutionEnvelope',
  'owner',
  'migration target',
  'remove_by',
  'kill_condition'
)
if (Test-Path $charterPath) {
  $charterText = Get-Content -Raw -Encoding UTF8 $charterPath
  foreach ($pattern in $charterPatterns) {
    if ($charterText -notmatch [regex]::Escape($pattern)) {
      $issues += "charter missing pattern: $pattern"
    }
  }
}

$activeFallbackIds = @()
$activeFallbackCount = 0
$localLegacyActiveMatches = @()
$canonicalApiTests = @(
  "apps/glue-python/tests/test_app.py::AppRouteTests::test_workflow_app_routes_store_backend_owned_registry_entries",
  "apps/glue-python/tests/test_app.py::AppRouteTests::test_workflow_version_routes_store_and_compare_backend_owned_snapshots",
  "apps/glue-python/tests/test_app.py::AppRouteTests::test_workflow_version_routes_accept_node_config_semantics_owned_by_desktop",
  "apps/glue-python/tests/test_app.py::AppRouteTests::test_run_reference_rejects_legacy_payload_fields"
)

if (Test-Path $inventoryPath) {
  $inventory = Get-Content -Raw -Encoding UTF8 $inventoryPath | ConvertFrom-Json
  if ([string]($inventory.schema_version) -ne "fallback_inventory.v1") {
    $issues += "fallback inventory schema_version must be fallback_inventory.v1"
  }
  if ([string]($inventory.authority_doc) -ne "docs/architecture_authority_charter_20260425.md") {
    $issues += "fallback inventory authority_doc drift"
  }
  $entries = @($inventory.entries)
  if ($entries.Count -lt 1) {
    $issues += "fallback inventory must contain at least one entry"
  }
  $today = Get-Date
  foreach ($entry in $entries) {
    $requiredFields = @("id", "status", "class", "owner", "scope", "reason", "added_at", "remove_by", "success_metric", "kill_condition")
    foreach ($field in $requiredFields) {
      $value = $entry.$field
      if ([string]::IsNullOrWhiteSpace([string]$value)) {
        $issues += "fallback inventory entry missing ${field}: $($entry.id)"
      }
    }
    if (-not ($entry.evidence -is [System.Collections.IEnumerable]) -or @($entry.evidence).Count -lt 1) {
      $issues += "fallback inventory entry missing evidence: $($entry.id)"
    }
    if (-not ($entry.match_patterns -is [System.Collections.IEnumerable]) -or @($entry.match_patterns).Count -lt 1) {
      $issues += "fallback inventory entry missing match_patterns: $($entry.id)"
    }
    if ([string]($entry.status) -eq "active") {
      $activeFallbackIds += [string]$entry.id
      try {
        $removeBy = [datetime]::ParseExact([string]$entry.remove_by, "yyyy-MM-dd", [System.Globalization.CultureInfo]::InvariantCulture)
        if ($removeBy.Date -lt $today.Date) {
          $issues += "active fallback past remove_by: $($entry.id) -> $($entry.remove_by)"
        }
      } catch {
        $issues += "active fallback remove_by must use yyyy-MM-dd: $($entry.id)"
      }
    }
  }
  $activeFallbackCount = @($activeFallbackIds).Count
}

$includeExtensions = @(".js", ".cjs", ".mjs", ".ts", ".tsx", ".py", ".java", ".rs", ".md", ".json", ".ps1", ".cs")
$candidateFiles = Get-ChildItem -Path $RepoRoot -Recurse -File -ErrorAction SilentlyContinue | Where-Object {
  $includeExtensions -contains $_.Extension.ToLowerInvariant() `
    -and $_.FullName -notmatch "\\\.git\\" `
    -and $_.FullName -notmatch "\\node_modules\\" `
    -and $_.FullName -notmatch "\\dist\\" `
    -and $_.FullName -notmatch "\\target\\" `
    -and $_.FullName -notmatch "\\bin\\" `
    -and $_.FullName -notmatch "\\obj\\" `
    -and $_.FullName -notmatch "\\ops\\logs\\" `
    -and $_.FullName -notmatch "\\docs\\archive\\" `
    -and $_.FullName -notmatch "check_architecture_convergence\.ps1$" `
    -and $_.FullName -notmatch "ci_check\.ps1$"
}
foreach ($file in $candidateFiles) {
  $text = Get-Content -Raw -Encoding UTF8 $file.FullName
  if ($text -match "local_legacy") {
    $localLegacyActiveMatches += $file.FullName
  }
}
if (@($localLegacyActiveMatches).Count -gt 0) {
  $issues += "active repo files still reference local_legacy: $(@($localLegacyActiveMatches) -join ', ')"
}

Push-Location $RepoRoot
try {
  $oldPythonPath = $env:PYTHONPATH
  $env:PYTHONPATH = Join-Path $RepoRoot "apps\glue-python"
  & python -m pytest @canonicalApiTests -q
  if ($LASTEXITCODE -ne 0) {
    $issues += "canonical workflow API contract tests failed"
  }
} finally {
  if ($null -eq $oldPythonPath) {
    Remove-Item Env:PYTHONPATH -ErrorAction SilentlyContinue
  } else {
    $env:PYTHONPATH = $oldPythonPath
  }
  Pop-Location
}

$status = if ($issues.Count -gt 0) { "failed" } else { "passed" }
$payload = [ordered]@{
  status = $status
  charterPath = $charterPath
  docsReadmePath = $docsReadmePath
  repoReadmePath = $repoReadmePath
  inventoryPath = $inventoryPath
  activeFallbackCount = $activeFallbackCount
  activeFallbackIds = @($activeFallbackIds | Sort-Object -Unique)
  localLegacyActiveMatches = @($localLegacyActiveMatches | Sort-Object -Unique)
  canonicalApiTests = $canonicalApiTests
  issues = @($issues | Sort-Object -Unique)
}

Write-Output ($payload | ConvertTo-Json -Compress -Depth 6)
if ($status -eq "failed") {
  exit 1
}

Ok "architecture convergence checks passed"
