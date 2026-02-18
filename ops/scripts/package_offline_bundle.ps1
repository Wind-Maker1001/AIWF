param(
  [string]$OutDir = "",
  [string]$Version = "",
  [ValidateSet("installer", "portable")]
  [string]$PackageType = "installer",
  [switch]$IncludeBundledTools,
  [switch]$CollectBundledTools,
  [switch]$CleanOldReleases,
  [bool]$RequireChineseOcr = $true,
  [string]$ReleaseChannel = "stable"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$distDir = Join-Path $root "apps\dify-desktop\dist"
$exePattern = if ($PackageType -eq "installer") { "AIWF Dify Desktop Setup *.exe" } else { "AIWF Dify Desktop *.exe" }
$exe = Get-ChildItem $distDir -File -Filter $exePattern |
  Where-Object {
    $_.Name -notlike "*.blockmap" -and
    (($PackageType -eq "installer" -and $_.Name -like "*Setup*") -or ($PackageType -eq "portable" -and $_.Name -notlike "*Setup*"))
  } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1
if (-not $exe) {
  throw "$PackageType exe not found under: $distDir"
}

if (-not $Version) {
  if ($exe.Name -match 'Desktop\s+(.+)\.exe$') {
    $Version = $Matches[1].Trim()
  } else {
    $Version = "latest"
  }
}

if (-not $OutDir) {
  $OutDir = Join-Path $root ("release\offline_bundle_{0}_{1}" -f $Version, $PackageType)
}

$bundleRoot = Join-Path $OutDir "AIWF_Offline_Bundle"
$docsOut = Join-Path $bundleRoot "docs"
if ($CleanOldReleases) {
  $releaseRoot = Join-Path $root "release"
  if (Test-Path $releaseRoot) {
    Get-ChildItem $releaseRoot -Directory -Filter "offline_bundle_*" |
      Where-Object { $_.FullName -ne $OutDir } |
      ForEach-Object { Remove-Item $_.FullName -Recurse -Force }
  }
}

Info "preparing output dir: $bundleRoot"
if (Test-Path $bundleRoot) {
  Remove-Item $bundleRoot -Recurse -Force
}
New-Item -ItemType Directory -Path $docsOut -Force | Out-Null

Info "copy desktop exe"
Copy-Item $exe.FullName (Join-Path $bundleRoot $exe.Name)
$blockmap = "$($exe.FullName).blockmap"
if (Test-Path $blockmap) {
  Copy-Item $blockmap (Join-Path $bundleRoot (Split-Path $blockmap -Leaf))
}

$docList = @(
  "docs\quickstart_desktop_offline.md",
  "docs\dify_desktop_app.md",
  "docs\offline_delivery_minimal.md",
  "docs\v1_1_goal_freeze.md",
  "docs\regression_quality.md"
)
foreach ($d in $docList) {
  $src = Join-Path $root $d
  if (Test-Path $src) {
    Copy-Item $src (Join-Path $docsOut (Split-Path $src -Leaf))
  }
}

if ($IncludeBundledTools) {
  if ($CollectBundledTools) {
    $collector = Join-Path $root "ops\scripts\collect_offline_tools.ps1"
    if (Test-Path $collector) {
      Info "collect bundled tools from local machine"
      powershell -ExecutionPolicy Bypass -File $collector -DesktopDir (Join-Path $root "apps\dify-desktop")
    }
  }
  $toolSrc = Join-Path $root "apps\dify-desktop\tools"
  $toolDst = Join-Path $bundleRoot "tools"
  if (Test-Path $toolSrc) {
    Info "copy bundled tools"
    Copy-Item $toolSrc $toolDst -Recurse -Force
    if ($RequireChineseOcr) {
      $chi = Join-Path $toolDst "tesseract\tessdata\chi_sim.traineddata"
      if (-not (Test-Path $chi)) {
        throw "chi_sim.traineddata missing in bundled tools: $chi"
      }
    }
  } else {
    Info "bundled tools source not found, skip: $toolSrc"
  }
}

$lines = @(
  "# AIWF 离线交付包",
  "",
  "## 内容",
  "- 可执行文件: $($exe.Name)",
  "- 包类型: $PackageType",
  "- 发布通道: $ReleaseChannel",
  "- 文档目录: docs/",
  "- 版本目录: $(Split-Path $OutDir -Leaf)",
  "",
  "## 安装与使用",
  "1. 双击运行 exe。",
  "2. 启动桌面应用。",
  "3. 保持在 离线本地模式。",
  "4. 将生肉文件拖入任务队列后点击 开始生成。",
  "5. 若包含 tools/，应用会优先使用内置 OCR 依赖（tesseract/pdftoppm）。",
  "",
  "## 默认输出目录",
  "文档\\AIWF-Offline\\<job_id>\\artifacts"
)
$lines | Set-Content (Join-Path $bundleRoot "README.txt") -Encoding UTF8

$sha = Get-FileHash (Join-Path $bundleRoot $exe.Name) -Algorithm SHA256
("{0}  {1}" -f $sha.Hash, $exe.Name) | Set-Content (Join-Path $bundleRoot "SHA256SUMS.txt") -Encoding ASCII

$manifest = [ordered]@{
  product = "AIWF Dify Desktop"
  version = $Version
  package_type = $PackageType
  release_channel = $ReleaseChannel
  exe = $exe.Name
  generated_at = (Get-Date).ToString("s")
  docs = @((Get-ChildItem $docsOut -File | ForEach-Object { $_.Name }))
}
($manifest | ConvertTo-Json -Depth 5) | Set-Content (Join-Path $bundleRoot "manifest.json") -Encoding UTF8

$notes = @(
  "# Release Notes",
  "",
  "- Version: $Version",
  "- Channel: $ReleaseChannel",
  "- PackageType: $PackageType",
  "- BuiltAt: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))",
  "",
  "## Included",
  "- $(($manifest.docs -join ', '))",
  "- SHA256SUMS.txt",
  "- manifest.json"
)
$notes | Set-Content (Join-Path $bundleRoot "RELEASE_NOTES.md") -Encoding UTF8

Ok "offline bundle ready: $bundleRoot"
