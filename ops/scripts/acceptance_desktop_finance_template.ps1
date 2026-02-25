param(
  [string]$Root = "",
  [string]$SampleDir = "",
  [string]$OutputRoot = "",
  [switch]$CopyArtifactsToDesktop
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $Root) { $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot) }
if (-not $SampleDir) { $SampleDir = Join-Path $Root "examples\finance_raw_demo" }
if (-not $OutputRoot) { $OutputRoot = Join-Path $Root "ops\logs\acceptance\desktop_finance_template" }

$desktopDir = Join-Path $Root "apps\dify-desktop"
if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }
if (-not (Test-Path $SampleDir)) { throw "sample dir not found: $SampleDir" }

$sampleCsv = Join-Path $SampleDir "finance_sheet.csv"
if (-not (Test-Path $sampleCsv)) { throw "sample file not found: $sampleCsv" }
$sampleFiles = @($sampleCsv)
$samplePoster = Join-Path $Root "examples\evidence_raw_demo\poster.png"
if (Test-Path $samplePoster) { $sampleFiles += $samplePoster }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Info "running finance template precheck + cleaning acceptance"

$payloadObj = @{
  params = @{
    report_title = "finance template acceptance"
    input_files = $sampleFiles
    cleaning_template = "finance_report_v1"
    office_lang = "zh"
    office_theme = "assignment"
    office_quality_mode = "high"
  }
  output_root = $runDir
}
$payloadJson = $payloadObj | ConvertTo-Json -Depth 8 -Compress
$payloadPath = Join-Path $runDir "payload.json"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($payloadPath, $payloadJson, $utf8NoBom)

$nodeScript = @'
const fs = require("fs");
const { runOfflinePrecheck, runOfflineCleaning } = require("./offline_engine");
(async()=>{
  const payload = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  const precheck = await runOfflinePrecheck(payload);
  const result = await runOfflineCleaning(payload);
  process.stdout.write(JSON.stringify({ precheck, result }));
})().catch((e)=>{
  process.stderr.write(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
'@

Push-Location $desktopDir
try {
  $json = $nodeScript | node - $payloadPath
  if ($LASTEXITCODE -ne 0) { throw "finance template acceptance run failed" }
}
finally {
  Pop-Location
}

$parsed = $json | ConvertFrom-Json
$precheck = $parsed.precheck
$result = $parsed.result
if (-not $precheck.ok) { throw "precheck execution failed" }
if (-not $result.ok) { throw "cleaning execution failed" }
if (-not $precheck.precheck.ok) { throw "finance template precheck did not pass" }

$xlsx = $result.artifacts | Where-Object { $_.kind -eq "xlsx" } | Select-Object -First 1
$docx = $result.artifacts | Where-Object { $_.kind -eq "docx" } | Select-Object -First 1
$pptx = $result.artifacts | Where-Object { $_.kind -eq "pptx" } | Select-Object -First 1
if (-not $xlsx -or -not $docx -or -not $pptx) {
  throw "missing office artifacts in finance acceptance"
}

$qualityScript = Join-Path $PSScriptRoot "check_office_artifacts_quality.ps1"
Info "running office quality gate on finance acceptance artifacts"
$qualityOut = powershell -ExecutionPolicy Bypass -File $qualityScript -XlsxPath $xlsx.path -DocxPath $docx.path -PptxPath $pptx.path 2>&1
if ($LASTEXITCODE -ne 0) {
  $qualityOut | Write-Host
  throw "office quality gate failed on finance acceptance artifacts"
}

$reportPath = Join-Path $runDir "acceptance_report.md"
$pc = $precheck.precheck
$q = $result.quality
$lines = @()
$lines += "# Desktop Finance Template Acceptance"
$lines += ""
$lines += "- Time: $(Get-Date -Format o)"
$lines += "- SampleDir: $SampleDir"
$lines += "- Files: $($sampleFiles.Count)"
$lines += "- OutputRoot: $runDir"
$lines += "- Template: finance_report_v1"
$lines += "- JobId: $($result.job_id)"
$lines += ""
$lines += "## Precheck"
$lines += "- ok: $($pc.ok)"
$lines += "- input_rows: $($pc.input_rows)"
$lines += "- missing_required_fields: $([string]::Join(', ', @($pc.missing_required_fields)))"
$lines += "- amount_field: $($pc.amount_field)"
$lines += "- amount_convert_rate: $($pc.amount_convert_rate)"
$lines += "- quality_gate_ok: $($pc.quality_gate_ok)"
$lines += ""
$lines += "## Cleaning Quality"
$lines += "- input_rows: $($q.input_rows)"
$lines += "- output_rows: $($q.output_rows)"
$lines += "- filtered_rows: $($q.filtered_rows)"
$lines += "- invalid_rows: $($q.invalid_rows)"
$lines += "- duplicate_rows_removed: $($q.duplicate_rows_removed)"
$lines += ""
$lines += "## Artifacts"
$lines += "- xlsx: $($xlsx.path)"
$lines += "- docx: $($docx.path)"
$lines += "- pptx: $($pptx.path)"
$lines += ""
$lines += "## Quality Gate Output"
$lines += "~~~text"
$lines += ($qualityOut | ForEach-Object { $_.ToString() })
$lines += "~~~"

$joined = [string]::Join([Environment]::NewLine, $lines)
Set-Content -Path $reportPath -Value $joined -Encoding UTF8
Copy-Item $reportPath (Join-Path $OutputRoot "desktop_finance_template_latest.md") -Force

if ($CopyArtifactsToDesktop) {
  $desktop = [Environment]::GetFolderPath("Desktop")
  $dest = Join-Path $desktop "AIWF_Finance_Acceptance_$stamp"
  New-Item -ItemType Directory -Path $dest -Force | Out-Null
  Copy-Item $xlsx.path (Join-Path $dest "fin.xlsx") -Force
  Copy-Item $docx.path (Join-Path $dest "audit.docx") -Force
  Copy-Item $pptx.path (Join-Path $dest "deck.pptx") -Force
  Copy-Item $reportPath (Join-Path $dest "acceptance_report.md") -Force
  Info "copied finance acceptance artifacts to desktop: $dest"
}

Ok "desktop finance template acceptance passed"
Write-Host "report: $reportPath"
exit 0
