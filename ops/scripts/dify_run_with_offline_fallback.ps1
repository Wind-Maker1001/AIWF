param(
  [string]$EnvFile = "",
  [string]$BaseUrl = "",
  [string]$PayloadFile = "",
  [string]$OutputFile = "",
  [string]$FallbackOutputRoot = "",
  [int]$TimeoutSec = 180,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $PayloadFile) {
  $PayloadFile = Join-Path $root "ops\config\dify_run_cleaning.payload.example.json"
}
if (-not $OutputFile) {
  $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
  $OutputFile = Join-Path $root ("tmp\dify_run_with_fallback_{0}.json" -f $stamp)
}
if (-not $FallbackOutputRoot) {
  $FallbackOutputRoot = Join-Path $root "offline-jobs"
}

$replayScript = Join-Path $PSScriptRoot "dify_replay_run_cleaning.ps1"
$desktopDir = Join-Path $root "apps\dify-desktop"
if (-not (Test-Path $replayScript)) { throw "missing script: $replayScript" }
if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }
if (-not (Test-Path $PayloadFile)) { throw "payload file not found: $PayloadFile" }

function Ensure-Dir([string]$path) {
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path -Force | Out-Null
  }
}

function Add-InputFile([System.Collections.Generic.List[string]]$list, [string]$v) {
  $s = [string]$v
  $s = $s.Trim()
  if (-not $s) { return }
  if (Test-Path $s) {
    $item = Get-Item $s
    if (-not $item.PSIsContainer) {
      if (-not $list.Contains($item.FullName)) { $list.Add($item.FullName) }
    }
  }
}

function Add-InputDirFiles([System.Collections.Generic.List[string]]$list, [string]$dirPath) {
  $s = [string]$dirPath
  $s = $s.Trim()
  if (-not $s -or -not (Test-Path $s)) { return }
  $item = Get-Item $s
  if (-not $item.PSIsContainer) { return }
  $extAllow = @(".pdf", ".docx", ".txt", ".md", ".xlsx", ".csv", ".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff")
  Get-ChildItem -Path $item.FullName -File | ForEach-Object {
    if ($extAllow -contains $_.Extension.ToLowerInvariant()) {
      if (-not $list.Contains($_.FullName)) { $list.Add($_.FullName) }
    }
  }
}

function Get-PropValue([object]$obj, [string]$name) {
  if ($null -eq $obj) { return $null }
  $p = $obj.PSObject.Properties[$name]
  if ($null -eq $p) { return $null }
  return $p.Value
}

function Build-OfflinePayload([string]$sourcePayloadFile, [string]$outputRoot) {
  $raw = Get-Content -Path $sourcePayloadFile -Raw -Encoding UTF8
  $obj = $raw | ConvertFrom-Json
  $params = Get-PropValue $obj "params"
  if ($null -eq $params) { $params = @{} }

  $files = [System.Collections.Generic.List[string]]::new()
  Add-InputFile $files (Get-PropValue $params "input_csv_path")
  Add-InputFile $files (Get-PropValue $params "input_path")
  Add-InputFile $files (Get-PropValue $params "input_xlsx_path")
  Add-InputFile $files (Get-PropValue $params "input_docx_path")
  Add-InputFile $files (Get-PropValue $params "input_pdf_path")
  Add-InputDirFiles $files (Get-PropValue $params "input_pdf_dir")
  Add-InputDirFiles $files (Get-PropValue $params "input_dir")

  $inputFilesVal = Get-PropValue $params "input_files"
  if ($inputFilesVal) {
    if ($inputFilesVal -is [System.Array]) {
      $inputFilesVal | ForEach-Object { Add-InputFile $files $_ }
    } else {
      try {
        $arr = ConvertFrom-Json -InputObject ([string]$inputFilesVal)
        if ($arr -is [System.Array]) { $arr | ForEach-Object { Add-InputFile $files $_ } }
      } catch {
        ([string]$inputFilesVal).Split("`n") | ForEach-Object { Add-InputFile $files $_ }
      }
    }
  }

  $topicRaw = Get-PropValue $params "topic"
  $topic = if ($topicRaw) { [string]$topicRaw } else { "" }
  $title = if ($topic.Trim()) { "Dify回退离线清洗 - $topic" } else { "Dify回退离线清洗" }
  $officeLang = Get-PropValue $params "office_lang"
  $officeTheme = Get-PropValue $params "office_theme"
  $officeQuality = Get-PropValue $params "office_quality_mode"
  $offlineParams = @{
    report_title = $title
    office_lang = if ($officeLang) { [string]$officeLang } else { "zh" }
    office_theme = if ($officeTheme) { [string]$officeTheme } else { "debate_plus" }
    office_quality_mode = if ($officeQuality) { [string]$officeQuality } else { "high" }
    input_files = ($files.ToArray() | ConvertTo-Json -Compress)
  }

  return @{
    params = $offlineParams
    output_root = $outputRoot
  }
}

function Invoke-OfflineFallback([string]$sourcePayloadFile, [string]$destOutputFile, [string]$outputRoot, [string]$fallbackReason) {
  Ensure-Dir (Split-Path -Parent $destOutputFile)
  Ensure-Dir $outputRoot
  $offlinePayload = Build-OfflinePayload -sourcePayloadFile $sourcePayloadFile -outputRoot $outputRoot
  $payloadJson = $offlinePayload | ConvertTo-Json -Depth 12 -Compress
  $payloadPath = Join-Path $root ("tmp\offline_fallback_payload_{0}.json" -f (Get-Date -Format "yyyyMMdd_HHmmssfff"))
  [System.IO.File]::WriteAllText($payloadPath, $payloadJson, (New-Object System.Text.UTF8Encoding($false)))

  $nodeScript = @'
const fs = require("fs");
const { runOfflineCleaning } = require("./offline_engine");
(async () => {
  const payload = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  const out = await runOfflineCleaning(payload);
  process.stdout.write(JSON.stringify(out));
})().catch((e) => {
  process.stderr.write(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
'@

  Push-Location $desktopDir
  try {
    $json = $nodeScript | node - $payloadPath
    if ($LASTEXITCODE -ne 0) { throw "offline fallback cleaning failed" }
  }
  finally {
    Pop-Location
  }

  $resp = $json | ConvertFrom-Json
  if (-not $resp.ok) { throw "offline fallback returned ok=false" }

  $wrapped = @{
    ok = $true
    mode = "offline_fallback"
    fallback_reason = $fallbackReason
    job_id = $resp.job_id
    run = $resp.run
    steps = @()
    artifacts = $resp.artifacts
    warnings = $resp.warnings
    quality = $resp.quality
    quality_gate = $resp.quality_gate
  }

  $outJson = $wrapped | ConvertTo-Json -Depth 20
  Set-Content -Path $destOutputFile -Value $outJson -Encoding UTF8
  return $wrapped
}

Write-Host ""
Write-Host "=== Dify Run With Offline Fallback ==="
Write-Host "payload    : $PayloadFile"
Write-Host "output     : $OutputFile"
Write-Host "fallback   : $FallbackOutputRoot"
Write-Host "dry_run    : $DryRun"

if ($DryRun) {
  Info "would call dify bridge; on error would fallback to offline engine"
  Ok "dry-run completed"
  exit 0
}

try {
  Info "attempting primary path: dify bridge run_cleaning"
  & $replayScript -EnvFile $EnvFile -BaseUrl $BaseUrl -PayloadFile $PayloadFile -OutputFile $OutputFile -TimeoutSec $TimeoutSec
  Ok "primary path succeeded"
  exit 0
}
catch {
  $reason = [string]$_.Exception.Message
  Warn ("primary path failed, switching to offline fallback: {0}" -f $reason)
}

$fallbackResp = Invoke-OfflineFallback -sourcePayloadFile $PayloadFile -destOutputFile $OutputFile -outputRoot $FallbackOutputRoot -fallbackReason "dify_bridge_failed"
$artifactCount = if ($fallbackResp.artifacts) { @($fallbackResp.artifacts).Count } else { 0 }

Write-Host ""
Write-Host "mode       : $($fallbackResp.mode)"
Write-Host "job_id     : $($fallbackResp.job_id)"
Write-Host "artifacts  : $artifactCount"
Write-Host "response   : $OutputFile"
Ok "offline fallback path succeeded"
