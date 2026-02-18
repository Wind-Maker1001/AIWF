param(
  [string]$DocsDir = "",
  [switch]$IncludeReadme
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DocsDir) {
  $DocsDir = Join-Path $root "docs"
}
if (-not (Test-Path $DocsDir)) {
  throw "docs dir not found: $DocsDir"
}

$targets = @(Get-ChildItem $DocsDir -Recurse -Filter *.md)
if ($IncludeReadme) {
  $readme = Join-Path $root "README.md"
  if (Test-Path $readme) { $targets += Get-Item $readme }
}

$issues = @()
foreach ($f in $targets) {
  $content = Get-Content $f.FullName -Raw
  $matches = [regex]::Matches($content, '\[[^\]]+\]\(([^)]+)\)')
  foreach ($m in $matches) {
    $rawTarget = $m.Groups[1].Value.Trim()
    if ($rawTarget -match '^(https?://|mailto:|#)') { continue }

    $target = $rawTarget.Split('#')[0]
    if ([string]::IsNullOrWhiteSpace($target)) { continue }

    $resolved = Join-Path $f.DirectoryName $target
    if (-not (Test-Path $resolved)) {
      $issues += [pscustomobject]@{
        file = $f.FullName.Substring($root.Length + 1)
        link = $rawTarget
      }
    }
  }
}

if ($issues.Count -eq 0) {
  Ok "no broken local markdown links"
  exit 0
}

Warn ("found {0} broken links" -f $issues.Count)
$issues | Sort-Object file, link | Format-Table -AutoSize
exit 2
