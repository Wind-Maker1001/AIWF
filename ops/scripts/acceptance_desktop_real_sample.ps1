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

Info "running desktop offline cleaning on real sample set"

$payloadObj = @{
  params = @{
    report_title = "AIWF assignment template v1 acceptance"
    input_files = ($sampleFiles | ConvertTo-Json -Compress)
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
const { runOfflineCleaning } = require("./offline_engine");
const fs = require("fs");
(async()=>{
  const payload = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  const out = await runOfflineCleaning(payload);
  process.stdout.write(JSON.stringify(out));
})().catch((e)=>{
  process.stderr.write(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
'@

Push-Location $desktopDir
try {
  $json = $nodeScript | node - $payloadPath
  if ($LASTEXITCODE -ne 0) { throw "desktop offline cleaning failed" }
}
finally {
  Pop-Location
}

$result = $json | ConvertFrom-Json
if (-not $result.ok) { throw "desktop offline cleaning result not ok" }

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
