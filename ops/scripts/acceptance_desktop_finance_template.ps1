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
. (Join-Path $PSScriptRoot "cleaning_shadow_acceptance_support.ps1")

if (-not $Root) { $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot) }
if (-not $SampleDir) { $SampleDir = Join-Path $Root "examples\finance_raw_demo" }
if (-not $OutputRoot) { $OutputRoot = Join-Path $Root "ops\logs\acceptance\desktop_finance_template" }

$desktopDir = Join-Path $Root "apps\dify-desktop"
if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }
if (-not (Test-Path $SampleDir)) { throw "sample dir not found: $SampleDir" }

$sampleCsv = Join-Path $SampleDir "finance_sheet.csv"
if (-not (Test-Path $sampleCsv)) { throw "sample file not found: $sampleCsv" }
$sampleFiles = @($sampleCsv)

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Info "running finance template precheck + cleaning acceptance"
$deps = Assert-CleaningShadowDependencies
$glueUrl = Get-AiwfGlueUrl
$jobId = [guid]::NewGuid().ToString("N")
$jobContext = New-AcceptanceJobContext -RunDir $runDir

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
$resultPath = Join-Path $runDir "cleaning_result.json"
$runModeAuditPath = Join-Path $runDir "run_mode_audit.jsonl"
$evidencePath = Join-Path $runDir "cleaning_shadow_rollout.json"
$latestResultPath = Join-Path $OutputRoot "cleaning_result.json"
$latestRunModeAuditPath = Join-Path $OutputRoot "run_mode_audit.jsonl"
$latestEvidencePath = Join-Path $OutputRoot "cleaning_shadow_rollout.json"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($payloadPath, $payloadJson, $utf8NoBom)

$nodeScript = @'
const fs = require("fs");
const { runOfflinePrecheck } = require("./offline_engine");
const {
  appendRunModeAuditEntry,
} = require("./cleaning_execution_audit");
(async()=>{
  const payload = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  const resultPath = process.argv[3];
  const runModeAuditPath = process.argv[4];
  const precheck = await runOfflinePrecheck(payload);
  process.stdout.write(JSON.stringify({ precheck }));
})().catch((e)=>{
  process.stderr.write(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
'@

$evidenceNodeScript = @'
const fs = require("fs");
const path = require("path");
const { buildCleaningShadowRolloutEvidence } = require("./cleaning_execution_audit");
(async()=>{
  const resultPath = process.argv[2];
  const runModeAuditPath = process.argv[3];
  const reportPath = process.argv[4];
  const evidencePath = process.argv[5];
  const result = JSON.parse(fs.readFileSync(resultPath, "utf8"));
  const evidence = buildCleaningShadowRolloutEvidence({
    acceptance: "desktop_finance_template",
    result,
    runModeAuditPath,
    reportPath,
    sampleResultPath: resultPath,
  });
  fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
  fs.writeFileSync(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`, "utf8");
})().catch((e)=>{
  process.stderr.write(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
'@

Push-Location $desktopDir
try {
  $json = $nodeScript | node - $payloadPath $resultPath $runModeAuditPath
  if ($LASTEXITCODE -ne 0) { throw "finance template precheck run failed" }
}
finally {
  Pop-Location
}

$parsed = $json | ConvertFrom-Json
$precheck = $parsed.precheck
if (-not $precheck.ok) { throw "precheck execution failed" }
if (-not $precheck.precheck.ok) { throw "finance template precheck did not pass" }

$params = [ordered]@{
  local_standalone = $true
  report_title = "finance template acceptance"
  input_csv_path = $sampleCsv
  cleaning_template = "finance_report_v1"
  office_lang = "zh"
  office_theme = "assignment"
  office_quality_mode = "high"
}
$startedAt = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$result = Invoke-ShadowCleaningMode {
  Invoke-GlueRunCleaningAcceptance `
    -GlueUrl $glueUrl `
    -JobId $jobId `
    -Actor "acceptance" `
    -RulesetVersion "v1" `
    -Params $params `
    -JobContext $jobContext `
    -TimeoutSec 300
}
Assert-ShadowCompareMatched -Result $result -Label "desktop_finance_template acceptance"
[System.IO.File]::WriteAllText($resultPath, (($result | ConvertTo-Json -Depth 20) + "`n"), $utf8NoBom)

$auditNodeScript = @'
const fs = require("fs");
const path = require("path");
const { appendRunModeAuditEntry } = require("./cleaning_execution_audit");
(() => {
  const resultPath = process.argv[2];
  const runModeAuditPath = process.argv[3];
  const startedAt = Number(process.argv[4] || Date.now());
  const out = JSON.parse(fs.readFileSync(resultPath, "utf8"));
  appendRunModeAuditEntry({
    fs,
    path,
    filePath: runModeAuditPath,
    mode: "glue_acceptance",
    result: out,
    startedAt,
  });
})();
'@

Push-Location $desktopDir
try {
  $null = $auditNodeScript | node - $resultPath $runModeAuditPath $startedAt
  if ($LASTEXITCODE -ne 0) { throw "finance template run mode audit write failed" }
}
finally {
  Pop-Location
}

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
$q = if ($result.PSObject.Properties.Name -contains "quality" -and $null -ne $result.quality) {
  $result.quality
} elseif ($result.PSObject.Properties.Name -contains "profile" -and $result.profile.PSObject.Properties.Name -contains "quality") {
  $result.profile.quality
} else {
  throw "finance acceptance result missing quality payload"
}
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
Copy-Item $resultPath $latestResultPath -Force
Copy-Item $runModeAuditPath $latestRunModeAuditPath -Force

Push-Location $desktopDir
try {
  $null = $evidenceNodeScript | node - $resultPath $runModeAuditPath $reportPath $evidencePath
  if ($LASTEXITCODE -ne 0) { throw "finance template rollout evidence write failed" }
}
finally {
  Pop-Location
}
Copy-Item $evidencePath $latestEvidencePath -Force

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
