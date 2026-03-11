param(
  [string]$Root = "",
  [string[]]$Docs = @("docs\release_notes_v1.1.6.md"),
  [string]$RequirePattern = "release/(gate_v1.1.6|v1.1.6)/"
)

$ErrorActionPreference = "Stop"

function Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m) { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$missing = @()
$checked = 0

foreach ($docRel in $Docs) {
  $doc = Join-Path $Root $docRel
  if (-not (Test-Path $doc)) {
    Warn "doc not found, skip: $docRel"
    continue
  }
  $raw = Get-Content -Raw -Encoding utf8 $doc
  $matches = [regex]::Matches($raw, '`(release\/[^`]+)`')
  foreach ($m in $matches) {
    $rel = $m.Groups[1].Value.Trim()
    if (-not $rel) { continue }
    if ($rel.Contains("<")) { continue }
    if ($RequirePattern -and ($rel -notmatch $RequirePattern)) { continue }
    $checked += 1
    $norm = $rel.Replace("/", "\")
    $full = Join-Path $Root $norm
    if (-not (Test-Path $full)) {
      $missing += $rel
    }
  }
}

if ($missing.Count -gt 0) {
  $uniq = $missing | Sort-Object -Unique
  Write-Host "[FAIL] release evidence missing:" -ForegroundColor Red
  foreach ($x in $uniq) {
    Write-Host "  - $x" -ForegroundColor Red
  }
  exit 1
}

Ok "release evidence paths verified ($checked checked)"
