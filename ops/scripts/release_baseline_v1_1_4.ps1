param(
  [string]$Version = "1.1.4",
  [string]$Channel = "stable",
  [switch]$SkipPackage,
  [switch]$SkipAcceptance,
  [switch]$CopyArtifactsToDesktop,
  [switch]$SkipHeavyGates
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$releaseDir = Join-Path $root ("release\v{0}" -f $Version)
New-Item -ItemType Directory -Path $releaseDir -Force | Out-Null

$releaseScript = Join-Path $PSScriptRoot "release_productize.ps1"
$acceptReal = Join-Path $PSScriptRoot "acceptance_desktop_real_sample.ps1"
$acceptFinance = Join-Path $PSScriptRoot "acceptance_desktop_finance_template.ps1"

$summary = [ordered]@{
  version = $Version
  generated_at = (Get-Date).ToString("o")
  package = "skipped"
  acceptance_real = "skipped"
  acceptance_finance = "skipped"
  notes = @()
}

if (-not $SkipPackage) {
  Info "packaging release baseline v$Version"
  $args = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $releaseScript,
    "-Version", $Version,
    "-Channel", $Channel
  )
  if ($SkipHeavyGates) {
    $args += "-SkipSqlConnectivityGate"
    $args += "-SkipRoutingBenchGate"
    $args += "-SkipRustTransformBenchGate"
  }
  powershell @args
  if ($LASTEXITCODE -ne 0) { throw "release_productize failed" }
  $summary.package = "passed"
  Ok "package baseline passed"
}

if (-not $SkipAcceptance) {
  Info "running desktop real-sample acceptance"
  $argsReal = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $acceptReal,
    "-OutputRoot", (Join-Path $releaseDir "acceptance_real")
  )
  if ($CopyArtifactsToDesktop) { $argsReal += "-CopyArtifactsToDesktop" }
  powershell @argsReal
  if ($LASTEXITCODE -ne 0) { throw "acceptance_desktop_real_sample failed" }
  $summary.acceptance_real = "passed"

  Info "running desktop finance-template acceptance"
  $argsFin = @(
    "-ExecutionPolicy", "Bypass",
    "-File", $acceptFinance,
    "-OutputRoot", (Join-Path $releaseDir "acceptance_finance")
  )
  if ($CopyArtifactsToDesktop) { $argsFin += "-CopyArtifactsToDesktop" }
  powershell @argsFin
  if ($LASTEXITCODE -ne 0) { throw "acceptance_desktop_finance_template failed" }
  $summary.acceptance_finance = "passed"
  Ok "acceptance baseline passed"
}

$checklistPath = Join-Path $releaseDir "clean_windows_checklist.md"
$lines = @()
$lines += "# AIWF v$Version Clean Windows 验收清单"
$lines += ""
$lines += "- 生成时间: $((Get-Date).ToString('o'))"
$lines += "- 版本: v$Version"
$lines += "- 目标: 纯净 Windows 安装后双击可用、拖拽文件可跑、产物质量可验收"
$lines += ""
$lines += "## 安装验收"
$lines += ("1. 运行 `"AIWF Dify Desktop Setup {0}.exe`"" -f $Version)
$lines += "2. 手动选择安装路径并完成安装"
$lines += "3. 双击桌面快捷方式，确认主界面正常打开"
$lines += "4. 确认没有额外 PowerShell 黑窗残留"
$lines += ""
$lines += "## 功能验收"
$lines += "1. 拖入 PDF/DOCX/TXT/XLSX/图片混合文件"
$lines += "2. 运行一次默认清洗流程"
$lines += "3. 检查 `"quality_report.md`" 中抽取率、乱码疑似率、注释剔除率、章节完整度"
$lines += "4. 当质量门禁失败时，检查是否自动切换 `"text_fidelity`" 模式"
$lines += ""
$lines += "## Workflow 验收"
$lines += "1. 打开 Workflow Studio，检查版本列表/回滚/审计日志"
$lines += "2. 检查性能看板（错误率/P95/重试率/Fallback率）"
$lines += "3. 检查离线能力边界提示与当前流程一致"
$lines += ""
$lines += "## 自动化结果"
$lines += "- package: $($summary.package)"
$lines += "- acceptance_real: $($summary.acceptance_real)"
$lines += "- acceptance_finance: $($summary.acceptance_finance)"
$lines += ""
Set-Content -Path $checklistPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

$summaryPath = Join-Path $releaseDir "baseline_summary.json"
($summary | ConvertTo-Json -Depth 5) | Set-Content -Path $summaryPath -Encoding UTF8

Ok "baseline summary: $summaryPath"
Ok "checklist: $checklistPath"
exit 0
