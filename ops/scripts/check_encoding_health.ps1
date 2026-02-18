param(
  [string]$Root = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$includeExt = @(".js",".cjs",".mjs",".ts",".tsx",".py",".java",".rs",".md",".sql",".yaml",".yml",".json",".html",".css",".ps1")
$excludeSeg = @("\.git\", "\node_modules\", "\dist\", "\dist-lite\", "\target\", "\.venv\", "\release\offline_bundle_", "\tmp\")

function IsExcluded([string]$p) {
  foreach ($seg in $excludeSeg) {
    if ($p -like "*$seg*") { return $true }
  }
  return $false
}

$all = Get-ChildItem -Path $Root -Recurse -File -ErrorAction SilentlyContinue
$files = $all | Where-Object {
  $ext = [System.IO.Path]::GetExtension($_.FullName).ToLowerInvariant()
  ($includeExt -contains $ext) -and (-not (IsExcluded $_.FullName))
}

if (-not $files -or $files.Count -eq 0) {
  Warn "no candidate files found"
  exit 0
}

Info ("checking encoding health for {0} files" -f $files.Count)

$issues = @()
$warns = @()
$mojibakePattern = "(?:Ã.|Â.|â.|ð.)"
$strictMojibake = $true
$strictEnv = ""
if ($null -ne $env:AIWF_ENCODING_STRICT_MOJIBAKE) {
  $strictEnv = [string]$env:AIWF_ENCODING_STRICT_MOJIBAKE
}
if ($strictEnv.ToLowerInvariant() -in @("0","false","no","off")) {
  $strictMojibake = $false
}
if ($strictEnv.ToLowerInvariant() -in @("1","true","yes","on")) {
  $strictMojibake = $true
}

foreach ($f in $files) {
  $text = [System.IO.File]::ReadAllText($f.FullName, [System.Text.Encoding]::UTF8)
  if ($text.Contains([char]0xFFFD)) {
    $issues += ("{0}: contains replacement char U+FFFD" -f $f.FullName)
  }
  if ($text -match $mojibakePattern) {
    $warns += ("{0}: contains likely mojibake glyphs" -f $f.FullName)
  }
}

if ($issues.Count -gt 0) {
  Write-Host ""
  Warn ("encoding health failed, issues={0}" -f $issues.Count)
  $issues | Select-Object -First 60 | ForEach-Object { Write-Host (" - {0}" -f $_) }
  if ($issues.Count -gt 60) {
    Write-Host (" ... and {0} more" -f ($issues.Count - 60))
  }
  exit 1
}

if ($warns.Count -gt 0) {
  if ($strictMojibake) {
    Write-Host ""
    Warn ("encoding health failed (strict mojibake), issues={0}" -f $warns.Count)
    $warns | Select-Object -First 60 | ForEach-Object { Write-Host (" - {0}" -f $_) }
    if ($warns.Count -gt 60) {
      Write-Host (" ... and {0} more" -f ($warns.Count - 60))
    }
    exit 1
  }
  Write-Host ""
  Warn ("encoding health warnings={0} (set AIWF_ENCODING_STRICT_MOJIBAKE=1 to enforce)" -f $warns.Count)
  $warns | Select-Object -First 20 | ForEach-Object { Write-Host (" - {0}" -f $_) }
}

Ok "encoding health checks passed"
