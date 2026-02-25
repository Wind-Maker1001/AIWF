param(
  [string]$ReportPath = "",
  [string]$BaselinePath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ReportPath) {
  $ReportPath = Join-Path $root "ops\logs\regression\regression_quality_report.json"
}
if (-not $BaselinePath) {
  $BaselinePath = Join-Path $root "ops\config\regression_baseline.v1.json"
}
if (-not (Test-Path $ReportPath)) { throw "regression report not found: $ReportPath" }
if (-not (Test-Path $BaselinePath)) { throw "baseline file not found: $BaselinePath" }

$report = Get-Content $ReportPath -Raw | ConvertFrom-Json
$baseline = Get-Content $BaselinePath -Raw | ConvertFrom-Json

$cleanQ = $report.cleaning.quality
$debateRows = [int]$report.preprocess.debate_rows
$missingRatio = [double]$report.preprocess.required_missing_ratio

$checks = [ordered]@{
  cleaning_output_rows = ([int]$cleanQ.output_rows -ge [int]$baseline.min_cleaning_output_rows)
  cleaning_invalid_rows = ([int]$cleanQ.invalid_rows -le [int]$baseline.max_cleaning_invalid_rows)
  preprocess_debate_rows = ($debateRows -ge [int]$baseline.min_preprocess_debate_rows)
  preprocess_required_missing_ratio = ($missingRatio -le [double]$baseline.max_preprocess_required_missing_ratio)
}

$failed = @($checks.GetEnumerator() | Where-Object { -not $_.Value } | ForEach-Object { $_.Key })
$audit = [ordered]@{
  ok = ($failed.Count -eq 0)
  generated_at = (Get-Date).ToString("s")
  report = $ReportPath
  baseline = $BaselinePath
  checks = $checks
  failed = $failed
}
$out = Join-Path (Split-Path $ReportPath -Parent) "regression_baseline_gate.json"
($audit | ConvertTo-Json -Depth 6) | Set-Content $out -Encoding UTF8

if ($failed.Count -gt 0) {
  throw "regression baseline gate failed: $($failed -join ', '). audit: $out"
}

Info "baseline: $BaselinePath"
Ok "regression baseline gate passed"
Ok "audit: $out"
