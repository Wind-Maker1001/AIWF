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
$mojibakePattern = "(?:\u00C3.|\u00C2.|\u00E2.|\u00F0.)"
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

$latinMojibakeAllowedFiles = @(
  (Join-Path $Root "apps/dify-desktop/main_runtime_encoding.js"),
  (Join-Path $Root "apps/dify-desktop/offline_paper.js"),
  (Join-Path $Root "apps/dify-desktop/offline_text.js")
) | ForEach-Object { [System.IO.Path]::GetFullPath($_) }

$latinMojibakeAllowedFileSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($item in $latinMojibakeAllowedFiles) {
  $null = $latinMojibakeAllowedFileSet.Add($item)
}

$privateUseAllowedFiles = @(
  (Join-Path $Root "apps/dify-desktop/tests-node/active_copy_regression.test.js"),
  (Join-Path $Root "ops/scripts/check_encoding_health.ps1")
) | ForEach-Object { [System.IO.Path]::GetFullPath($_) }

$privateUseAllowedFileSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($item in $privateUseAllowedFiles) {
  $null = $privateUseAllowedFileSet.Add($item)
}

# Keep mojibake regex ASCII-only so Windows PowerShell 5.1 parses it
# consistently on non-Chinese runners.
$cjkMojibakePattern = '(?:\u5A75\u509B\u57B9\u7039.|\u95BA\u5D8F\u7E5D\u941B.|\u95BA\u5906\u665C\u9354\u6A40.|\u6FE1\uE0A3\u6F98\u701A\uE162\u02C9|\u95BA\u55D7\u5039\u59AB.|\u95BB\u65C2\u5595\u934A.|\u5A11\u64B9\u6AE3\u9239.|\u941E\u6D92\u5053\u7F02.|\u95B8\u2544\u6AE3\u9364.|\u6FDE\u7FE0\u80F6\u923B\u714E\uE18F)'
$cjkPrivateUsePattern = "[\uE000-\uF8FF]"

foreach ($f in $files) {
  $text = [System.IO.File]::ReadAllText($f.FullName, [System.Text.Encoding]::UTF8)
  $fullPath = [System.IO.Path]::GetFullPath($f.FullName)
  if ($text.Contains([char]0xFFFD)) {
    $issues += ("{0}: contains replacement char U+FFFD" -f $f.FullName)
  }
  if (($text -match $mojibakePattern) -and (-not $latinMojibakeAllowedFileSet.Contains($fullPath))) {
    $warns += ("{0}: contains likely mojibake glyphs" -f $f.FullName)
  }
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
