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
if (-not $SampleDir) { $SampleDir = Join-Path $Root "examples\evidence_raw_demo" }
if (-not $OutputRoot) { $OutputRoot = Join-Path $Root "ops\logs\acceptance\desktop_real_sample" }

if (-not (Test-Path $SampleDir)) { throw "sample dir not found: $SampleDir" }

$desktopDir = Join-Path $Root "apps\dify-desktop"
if (-not (Test-Path $desktopDir)) { throw "desktop dir not found: $desktopDir" }

New-Item -ItemType Directory -Path $OutputRoot -Force | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$runDir = Join-Path $OutputRoot $stamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$sampleFiles = @(
  (Join-Path $SampleDir "debate_notes.txt"),
  (Join-Path $SampleDir "hearing.docx"),
  (Join-Path $SampleDir "claims.xlsx"),
  (Join-Path $SampleDir "poster.png")
) | Where-Object { Test-Path $_ }
if ($sampleFiles.Count -lt 3) { throw "not enough sample files in $SampleDir" }

Info "checking glue/accel health for shadow acceptance"
$deps = Assert-CleaningShadowDependencies
$glueUrl = Get-AiwfGlueUrl

$jobId = [guid]::NewGuid().ToString("N")
$jobContext = New-AcceptanceJobContext -RunDir $runDir
$extractBody = [ordered]@{
  input_files = $sampleFiles
  ocr_enabled = $true
  xlsx_all_sheets = $true
  on_file_error = "raise"
}
$extract = Invoke-RestMethod -Uri ("{0}/ingest/extract" -f $glueUrl.TrimEnd("/")) -Method Post -ContentType "application/json" -Body ($extractBody | ConvertTo-Json -Depth 12) -TimeoutSec 300
if (-not [bool]$extract.ok) { throw "glue ingest extract returned ok=false" }
if ([bool]$extract.quality_blocked) {
  throw ("glue ingest extract quality_blocked=true: {0}" -f (($extract.blocked_reason_codes | ConvertTo-Json -Compress)))
}
if (-not ($extract.rows) -or @($extract.rows).Count -lt 1) { throw "glue ingest extract returned no rows" }
$params = [ordered]@{
  local_standalone = $true
  report_title = "AIWF assignment template v1 acceptance"
  rows = @($extract.rows)
  cleaning_template = "debate_evidence_v1"
  office_lang = "zh"
  office_theme = "assignment"
  office_quality_mode = "high"
  md_only = $false
  strict_output_gate = $false
  content_quality_gate_enabled = $false
  office_quality_gate_enabled = $false
  xlsx_embed_charts = $true
}
$payloadObj = [ordered]@{
  actor = "acceptance"
  ruleset_version = "v1"
  params = $params
  job_context = $jobContext
}
$payloadJson = $payloadObj | ConvertTo-Json -Depth 16 -Compress
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
const { appendRunModeAuditEntry } = require("./cleaning_execution_audit");
const fs = require("fs");
const path = require("path");
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

$evidenceNodeScript = @'
const fs = require("fs");
const path = require("path");
const {
  buildCleaningShadowRolloutEvidence,
} = require("./cleaning_execution_audit");
(async()=>{
  const resultPath = process.argv[2];
  const runModeAuditPath = process.argv[3];
  const reportPath = process.argv[4];
  const evidencePath = process.argv[5];
  const result = JSON.parse(fs.readFileSync(resultPath, "utf8"));
  const evidence = buildCleaningShadowRolloutEvidence({
    acceptance: "desktop_real_sample",
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

Info "running glue cleaning on real sample set"
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
Assert-ShadowCompareMatched -Result $result -Label "desktop_real_sample acceptance"
[System.IO.File]::WriteAllText($resultPath, (($result | ConvertTo-Json -Depth 20) + "`n"), $utf8NoBom)

$qualitySummary = if ($result.PSObject.Properties.Name -contains "quality_summary" -and $null -ne $result.quality_summary) {
  $result.quality_summary
} else {
  $null
}
$q = if ($result.PSObject.Properties.Name -contains "quality" -and $null -ne $result.quality) {
  $result.quality
} elseif ($result.PSObject.Properties.Name -contains "profile" -and $result.profile.PSObject.Properties.Name -contains "quality") {
  $result.profile.quality
} else {
  throw "desktop real sample result missing quality payload"
}
if ($null -eq $qualitySummary) {
  throw "desktop real sample result missing quality_summary"
}
if (-not [bool]$qualitySummary.blank_output_expected -and [int]$q.output_rows -le 0) {
  throw "desktop real sample produced zero rows while blank_output_expected=false"
}
if ([bool]$qualitySummary.zero_output_unexpected) {
  throw "desktop real sample quality_summary.zero_output_unexpected=true"
}

Push-Location $desktopDir
try {
  $null = $nodeScript | node - $resultPath $runModeAuditPath $startedAt
  if ($LASTEXITCODE -ne 0) { throw "desktop real sample run mode audit write failed" }
}
finally {
  Pop-Location
}

$xlsx = $result.artifacts | Where-Object { $_.kind -eq "xlsx" } | Select-Object -First 1
$docx = $result.artifacts | Where-Object { $_.kind -eq "docx" } | Select-Object -First 1
$pptx = $result.artifacts | Where-Object { $_.kind -eq "pptx" } | Select-Object -First 1
if (-not $xlsx -or -not $docx -or -not $pptx) {
  throw "missing office artifacts in acceptance run"
}

$qualityScript = Join-Path $PSScriptRoot "check_office_artifacts_quality.ps1"
Info "running office quality gate on acceptance artifacts"
$qualityOut = powershell -ExecutionPolicy Bypass -File $qualityScript -XlsxPath $xlsx.path -DocxPath $docx.path -PptxPath $pptx.path 2>&1
if ($LASTEXITCODE -ne 0) {
  $qualityOut | Write-Host
  throw "office quality gate failed on acceptance artifacts"
}

$reportPath = Join-Path $runDir "acceptance_report.md"
$lines = @()
$lines += "# Desktop Real Sample Acceptance"
$lines += ""
$lines += "- Time: $(Get-Date -Format o)"
$lines += "- SampleDir: $SampleDir"
$lines += "- OutputRoot: $runDir"
$lines += "- Files: $($sampleFiles.Count)"
$lines += "- JobId: $($result.job_id)"
$lines += "- Theme: assignment"
$lines += "- QualityMode: high"
$lines += ""
$lines += "## Cleaning Quality"
$lines += "- input_rows: $($q.input_rows)"
$lines += "- output_rows: $($q.output_rows)"
$lines += "- filtered_rows: $($q.filtered_rows)"
$lines += "- invalid_rows: $($q.invalid_rows)"
$lines += "- duplicate_rows_removed: $($q.duplicate_rows_removed)"
$lines += "- requested_profile: $($qualitySummary.requested_profile)"
$lines += "- recommended_profile: $($qualitySummary.recommended_profile)"
$lines += "- blocking_reason_codes: $([string]::Join(', ', @($qualitySummary.blocking_reason_codes)))"
$lines += "- blank_output_expected: $($qualitySummary.blank_output_expected)"
$lines += "- zero_output_unexpected: $($qualitySummary.zero_output_unexpected)"
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
Copy-Item $reportPath (Join-Path $OutputRoot "desktop_real_sample_latest.md") -Force
Copy-Item $resultPath $latestResultPath -Force
Copy-Item $runModeAuditPath $latestRunModeAuditPath -Force

Push-Location $desktopDir
try {
  $null = $evidenceNodeScript | node - $resultPath $runModeAuditPath $reportPath $evidencePath
  if ($LASTEXITCODE -ne 0) { throw "desktop real sample rollout evidence write failed" }
}
finally {
  Pop-Location
}
Copy-Item $evidencePath $latestEvidencePath -Force

if ($CopyArtifactsToDesktop) {
  $desktop = [Environment]::GetFolderPath("Desktop")
  $dest = Join-Path $desktop "AIWF_Acceptance_$stamp"
  New-Item -ItemType Directory -Path $dest -Force | Out-Null
  Copy-Item $xlsx.path (Join-Path $dest "fin.xlsx") -Force
  Copy-Item $docx.path (Join-Path $dest "audit.docx") -Force
  Copy-Item $pptx.path (Join-Path $dest "deck.pptx") -Force
  Copy-Item $reportPath (Join-Path $dest "acceptance_report.md") -Force
  Info "copied artifacts to desktop: $dest"
}

Ok "desktop real sample acceptance passed"
Write-Host "report: $reportPath"
exit 0
