param(
  [string]$EnvFile = "",
  [string]$Owner = "local",
  [int]$Rows = 20000,
  [int]$OfficeMaxRows = 5000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $EnvFile) {
  $root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
  $EnvFile = Join-Path $root "ops\config\dev.env"
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }
    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

Import-DotEnv $EnvFile

$base = if ($env:AIWF_BASE_URL) { $env:AIWF_BASE_URL } else { "http://127.0.0.1:18080" }
$workDir = Join-Path $env:TEMP "aiwf-perf"
New-Item -Path $workDir -ItemType Directory -Force | Out-Null
$csvPath = Join-Path $workDir ("perf_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".csv")

Info "generating CSV rows=$Rows at $csvPath"
$swWrite = [System.Diagnostics.Stopwatch]::StartNew()
$writer = New-Object System.IO.StreamWriter($csvPath, $false, [System.Text.Encoding]::UTF8)
try {
  $writer.WriteLine("id,amount")
  for ($i = 1; $i -le $Rows; $i++) {
    $amt = (($i * 13) % 10000) + 0.25
    $writer.WriteLine("$i,$amt")
  }
}
finally {
  $writer.Dispose()
  $swWrite.Stop()
}
Ok ("csv generated in {0:n2}s" -f $swWrite.Elapsed.TotalSeconds)

$job = Invoke-RestMethod "$base/api/v1/tools/create_job?owner=$Owner" -Method Post -ContentType "application/json" -Body "{}"
$jobId = $job.job_id
Info "created job_id=$jobId"

$runBody = @{
  actor = "local"
  ruleset_version = "v1"
  params = @{
    input_csv_path = $csvPath
    office_max_rows = $OfficeMaxRows
    office_lang = "zh"
    office_theme = "professional"
  }
} | ConvertTo-Json -Depth 6

$swRun = [System.Diagnostics.Stopwatch]::StartNew()
$run = Invoke-RestMethod "$base/api/v1/jobs/$jobId/run/cleaning" -Method Post -ContentType "application/json" -Body $runBody
$swRun.Stop()

$xlsxArtifact = $run.artifacts | Where-Object { $_.kind -eq "xlsx" } | Select-Object -First 1
if (-not $xlsxArtifact) {
  throw "xlsx artifact not found"
}

$pyCheck = @'
import json, sys
import os
from openpyxl import load_workbook
p = os.environ["AIWF_XLSX_PATH"]
wb = load_workbook(p, read_only=True, data_only=True)
try:
    ws = wb.active
    rows = max(0, int(ws.max_row or 0) - 1)
    print(json.dumps({"xlsx_data_rows": rows}, ensure_ascii=False))
finally:
    wb.close()
'@
$env:AIWF_XLSX_PATH = $xlsxArtifact.path
$xlsxJson = $pyCheck | python -
$env:AIWF_XLSX_PATH = $null
$xlsxStat = $xlsxJson | ConvertFrom-Json
$xlsxRows = [int]$xlsxStat.xlsx_data_rows

Write-Host ""
Write-Host "=== Office Perf Result ==="
Write-Host "job_id            : $jobId"
Write-Host "input_rows        : $Rows"
Write-Host "office_max_rows   : $OfficeMaxRows"
Write-Host "xlsx_data_rows    : $xlsxRows"
Write-Host "run_seconds_api   : $($run.seconds)"
Write-Host "run_seconds_local : $([math]::Round($swRun.Elapsed.TotalSeconds, 3))"
Write-Host "xlsx_path         : $($xlsxArtifact.path)"

if ($xlsxRows -gt $OfficeMaxRows) {
  throw "xlsx row count exceeds office_max_rows ($xlsxRows > $OfficeMaxRows)"
}

Ok "large-dataset office guardrail verified"
