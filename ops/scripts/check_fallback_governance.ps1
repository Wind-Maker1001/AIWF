param(
  [string]$RepoRoot = "",
  [string]$Today = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $Today) {
  $Today = (Get-Date).ToString("yyyy-MM-dd")
}

$docPath = Join-Path $RepoRoot "docs\fallback_governance_20260320.md"
if (-not (Test-Path $docPath)) {
  throw "fallback governance doc missing: $docPath"
}

$content = Get-Content $docPath -Raw -Encoding UTF8
$lines = $content -split "`r?`n"
$errors = New-Object System.Collections.Generic.List[string]

function Add-Error([string]$Message) {
  $errors.Add($Message)
}

function Get-SectionBody([string]$Title) {
  $start = -1
  for ($i = 0; $i -lt $lines.Length; $i++) {
    if ($lines[$i] -eq "### $Title" -or $lines[$i] -eq "## $Title") {
      $start = $i + 1
      break
    }
  }
  if ($start -lt 0) { return $null }
  $bodyLines = New-Object System.Collections.Generic.List[string]
  for ($i = $start; $i -lt $lines.Length; $i++) {
    if ($lines[$i].StartsWith("### ") -or $lines[$i].StartsWith("## ")) { break }
    $bodyLines.Add($lines[$i])
  }
  return [string]::Join("`n", $bodyLines)
}

$desktopRoot = Join-Path $RepoRoot "apps\dify-desktop"

function Get-RelativeRepoPath([string]$AbsolutePath) {
  $root = [System.IO.Path]::GetFullPath($RepoRoot).TrimEnd('\', '/')
  $path = [System.IO.Path]::GetFullPath($AbsolutePath)
  if ($path.StartsWith($root, [System.StringComparison]::OrdinalIgnoreCase)) {
    return $path.Substring($root.Length).TrimStart('\', '/').Replace('/', '\')
  }
  return $AbsolutePath
}

function Get-LocalLegacyFallbackSections() {
  $providerFiles = Get-ChildItem -Path $desktopRoot -Recurse -Filter *.js -File |
    Where-Object {
      $_.FullName -notmatch '[\\/]tests-node[\\/]' -and
      $_.FullName -notmatch '[\\/]node_modules[\\/]'
    }

  $sections = New-Object System.Collections.Generic.List[object]
  foreach ($file in $providerFiles) {
    $fileText = Get-Content $file.FullName -Raw -Encoding UTF8
    if ($fileText -notmatch '(?m)^const LOCAL_PROVIDER = "local_legacy";\s*$') {
      continue
    }

    $titleMatch = [regex]::Match($fileText, '(?m)^const FALLBACK_GOVERNANCE_TITLE = (?:"([^"]+)"|''([^'']+)'');\s*$')
    if (-not $titleMatch.Success) {
      Add-Error("local_legacy provider missing FALLBACK_GOVERNANCE_TITLE marker: $($file.FullName)")
      continue
    }

    $title = if ($titleMatch.Groups[1].Success) { $titleMatch.Groups[1].Value } else { $titleMatch.Groups[2].Value }
    $sections.Add(@{
      Title = $title
      File = (Get-RelativeRepoPath $file.FullName)
      RequireLocalLegacy = $true
    })
  }

  return $sections
}

function Get-TitleManagedFallbackSections() {
  $providerFiles = Get-ChildItem -Path $desktopRoot -Recurse -Filter *.js -File |
    Where-Object {
      $_.FullName -notmatch '[\\/]tests-node[\\/]' -and
      $_.FullName -notmatch '[\\/]node_modules[\\/]'
    }

  $sections = New-Object System.Collections.Generic.List[object]
  foreach ($file in $providerFiles) {
    $fileText = Get-Content $file.FullName -Raw -Encoding UTF8
    $titleMatch = [regex]::Match($fileText, '(?m)^const FALLBACK_GOVERNANCE_TITLE = (?:"([^"]+)"|''([^'']+)'');\s*$')
    if (-not $titleMatch.Success) {
      continue
    }

    $title = if ($titleMatch.Groups[1].Success) { $titleMatch.Groups[1].Value } else { $titleMatch.Groups[2].Value }
    $requireLocalLegacy = $fileText -match '(?m)^const LOCAL_PROVIDER = "local_legacy";\s*$'
    $sections.Add(@{
      Title = $title
      File = (Get-RelativeRepoPath $file.FullName)
      RequireLocalLegacy = $requireLocalLegacy
    })
  }
  return $sections
}

$requiredLabels = @("owner", "reason", "added_at", "remove_by", "success_metric", "kill_condition")
$retiredLabels = @("retired_at", "owner", "removal_reason", "follow_up")
$staticSections = @(
  @{
    Title = '1. workflow.version migration on import / normalization'
    File = "apps\dify-desktop\renderer\workflow\workflow-contract.js"
    RequireLocalLegacy = $false
  }
)
$sections = @($staticSections + (Get-TitleManagedFallbackSections))
$titles = @{}
foreach ($section in $sections) {
  $title = [string]$section.Title
  if ($titles.ContainsKey($title)) {
    Add-Error("duplicate fallback governance title detected: $title")
  }
  else {
    $titles[$title] = $true
  }
}

$todayDate = [datetime]::ParseExact($Today, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)

foreach ($section in $sections) {
  $title = [string]$section.Title
  $filePath = Join-Path $RepoRoot ([string]$section.File)
  if (-not (Test-Path $filePath)) {
    Add-Error("fallback governance referenced file missing: $filePath")
    continue
  }

  $body = Get-SectionBody $title
  if ($null -eq $body) {
    Add-Error("fallback governance section missing: $title")
    continue
  }

  $isRetired = $body -match "(?m)^- retired_at:\s+\d{4}-\d{2}-\d{2}\s*$"
  $labelsToCheck = if ($isRetired) { $retiredLabels } else { $requiredLabels }

  foreach ($label in $labelsToCheck) {
    if ($body -notmatch ("(?m)^- " + [regex]::Escape($label) + ":\s+.+$")) {
      Add-Error("fallback governance section '$title' missing field: $label")
    }
  }

  if (-not $isRetired) {
    $removeByMatch = [regex]::Match($body, "(?m)^- remove_by:\s*(\d{4}-\d{2}-\d{2})\s*$")
    if (-not $removeByMatch.Success) {
      Add-Error("fallback governance section '$title' missing parseable remove_by date")
    } else {
      $removeByDate = [datetime]::ParseExact($removeByMatch.Groups[1].Value, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
      if ($removeByDate -lt $todayDate) {
        Add-Error("fallback governance section '$title' is past remove_by: $($removeByMatch.Groups[1].Value)")
      }
    }
  }

  $fileText = Get-Content $filePath -Raw -Encoding UTF8
  if ($section.RequireLocalLegacy -and $fileText -notmatch 'local_legacy') {
    Add-Error("fallback governance file does not expose local_legacy marker: $filePath")
  }
  if ($section.RequireLocalLegacy -and $fileText -notmatch 'FALLBACK_GOVERNANCE_TITLE') {
    Add-Error("fallback governance file does not expose FALLBACK_GOVERNANCE_TITLE marker: $filePath")
  }
}

if ($lines -notcontains '## Gate Direction') {
  Add-Error("fallback governance doc missing Gate Direction section")
}
else {
  $gateSection = Get-SectionBody "Gate Direction"
  if ($null -eq $gateSection) {
    Add-Error("fallback governance doc missing Gate Direction body")
  } else {
    if ($gateSection -notmatch 'owner' -or $gateSection -notmatch '`remove_by`') {
      Add-Error("fallback governance doc missing owner/remove_by gate rule")
    }
    if ($gateSection -notmatch 'adapter fallback') {
      Add-Error("fallback governance doc missing adapter fallback boundary rule")
    }
    if ($gateSection -notmatch 'FALLBACK_GOVERNANCE_TITLE') {
      Add-Error("fallback governance doc missing auto-discovery rule for local_legacy providers")
    }
  }
}

if ($errors.Count -gt 0) {
  throw ("fallback governance checks failed:`n- " + (($errors | Select-Object -Unique) -join "`n- "))
}

Ok "fallback governance check passed"
