param(
  [string]$Root = "",
  [string[]]$Docs = @(
    "docs\release_notes_v1.1.6.md",
    "docs\offline_delivery_minimal.md"
  )
)

$ErrorActionPreference = "Stop"

function Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m) { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$evidenceSpecs = @(
  [pscustomobject]@{
    name = "release_gate_summary.json"
    docs = @("docs\release_notes_v1.1.6.md")
    scripts = @("ops\scripts\release_gate_v1_1_6.ps1")
  },
  [pscustomobject]@{
    name = "release_gate_summary.md"
    docs = @("docs\release_notes_v1.1.6.md")
    scripts = @("ops\scripts\release_gate_v1_1_6.ps1")
  },
  [pscustomobject]@{
    name = "manifest.json"
    docs = @("docs\offline_delivery_minimal.md", "docs\offline_delivery_native_winui.md")
    scripts = @("ops\scripts\package_offline_bundle.ps1", "ops\scripts\release_productize.ps1", "ops\scripts\package_native_winui_bundle.ps1", "ops\scripts\release_frontend_productize.ps1")
  },
  [pscustomobject]@{
    name = "RELEASE_NOTES.md"
    docs = @("docs\offline_delivery_minimal.md", "docs\offline_delivery_native_winui.md")
    scripts = @("ops\scripts\package_offline_bundle.ps1", "ops\scripts\release_productize.ps1", "ops\scripts\package_native_winui_bundle.ps1", "ops\scripts\release_frontend_productize.ps1")
  },
  [pscustomobject]@{
    name = "SHA256SUMS.txt"
    docs = @("docs\offline_delivery_minimal.md", "docs\offline_delivery_native_winui.md")
    scripts = @("ops\scripts\package_offline_bundle.ps1", "ops\scripts\release_productize.ps1", "ops\scripts\package_native_winui_bundle.ps1", "ops\scripts\release_frontend_productize.ps1")
  }
)

$docSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($doc in $Docs) { $null = $docSet.Add($doc) }
foreach ($spec in $evidenceSpecs) {
  foreach ($doc in $spec.docs) { $null = $docSet.Add($doc) }
}

$docCache = @{}
foreach ($docRel in $docSet) {
  $full = Join-Path $Root $docRel
  if (-not (Test-Path $full)) {
    throw "required release-evidence doc missing: $docRel"
  }
  $docCache[$docRel] = Get-Content -Raw -Encoding utf8 $full
}

$issues = @()
$checked = 0

foreach ($spec in $evidenceSpecs) {
  $docMatched = $false
  foreach ($docRel in $spec.docs) {
    $raw = [string]$docCache[$docRel]
    if ($raw -match [regex]::Escape($spec.name)) {
      $docMatched = $true
      $checked += 1
      break
    }
  }
  if (-not $docMatched) {
    $issues += "docs missing release evidence reference: $($spec.name)"
  }

  $scriptMatched = $false
  foreach ($scriptRel in $spec.scripts) {
    $script = Join-Path $Root $scriptRel
    if (-not (Test-Path $script)) {
      $issues += "release script missing: $scriptRel"
      continue
    }
    $raw = Get-Content -Raw -Encoding utf8 $script
    if ($raw -match [regex]::Escape($spec.name)) {
      $scriptMatched = $true
      $checked += 1
      break
    }
  }
  if (-not $scriptMatched) {
    $issues += "release scripts do not emit expected evidence file: $($spec.name)"
  }
}

if ($issues.Count -gt 0) {
  Write-Host "[FAIL] release evidence validation failed:" -ForegroundColor Red
  $issues | Sort-Object -Unique | ForEach-Object {
    Write-Host "  - $_" -ForegroundColor Red
  }
  exit 1
}

Ok "release evidence declarations verified ($checked checked)"
