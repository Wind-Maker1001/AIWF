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
$excludeSeg = @(
  "\.git\",
  "\node_modules\",
  "\node_modules_stale_",
  "\dist\",
  "\dist-lite\",
  "\target\",
  "\.venv\",
  "\bin\",
  "\obj\",
  "\release\offline_bundle_",
  "\tmp\"
)

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

$cjkMojibakeSourceFiles = @(
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/workflow-contract.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/preflight-controller-ui.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/support-ui-run-compare.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/support-ui-run-compare-renderer.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/graph-shell-ui.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/static-config.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/defaults-templates-core-data-pipeline.js"),
  (Join-Path $Root "apps/dify-desktop/renderer/workflow/run-payload-support.js"),
  (Join-Path $Root "infra/sqlserver/init/002_control_plane_extend.sql"),
  (Join-Path $Root "apps/dify-desktop/workflow_ipc_reports.js"),
  (Join-Path $Root "apps/dify-desktop/workflow_ipc_store.js")
) | ForEach-Object { [System.IO.Path]::GetFullPath($_) }

$cjkMojibakeFileSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($item in $cjkMojibakeSourceFiles) {
  $null = $cjkMojibakeFileSet.Add($item)
}

$privateUseAllowedFiles = @(
  (Join-Path $Root "apps/dify-desktop/tests-node/active_copy_regression.test.js"),
  (Join-Path $Root "ops/scripts/check_encoding_health.ps1")
) | ForEach-Object { [System.IO.Path]::GetFullPath($_) }

$privateUseAllowedFileSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($item in $privateUseAllowedFiles) {
  $null = $privateUseAllowedFileSet.Add($item)
}

$cjkMojibakePattern = '(?:婵傛垹瀹.|閺嶏繝鐛.|閺夆晜鍔橀.|濡澘瀚ˉ|閺嗗倹妫.|閻旂喕鍊.|娑撹櫣鈹.|鐞涒偓缂.|閸╄櫣鍤.|濞翠胶鈻煎)'
$cjkPrivateUsePattern = "[\uE000-\uF8FF]"

foreach ($f in $files) {
  $text = [System.IO.File]::ReadAllText($f.FullName, [System.Text.Encoding]::UTF8)
  if ($text.Contains([char]0xFFFD)) {
    $issues += ("{0}: contains replacement char U+FFFD" -f $f.FullName)
  }
  if ($text -match $mojibakePattern) {
    $warns += ("{0}: contains likely mojibake glyphs" -f $f.FullName)
  }
  $fullPath = [System.IO.Path]::GetFullPath($f.FullName)
  if (($text -match $cjkPrivateUsePattern) -and (-not $privateUseAllowedFileSet.Contains($fullPath))) {
    $warns += ("{0}: contains private-use glyphs" -f $f.FullName)
  }
  if ($cjkMojibakeFileSet.Contains($fullPath)) {
    if ($text -match $cjkMojibakePattern) {
      $warns += ("{0}: contains likely CJK mojibake patterns" -f $f.FullName)
    }
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
